package routes

import (
	"EverythingSuckz/fsb/config"
	"EverythingSuckz/fsb/internal/bot"
	"EverythingSuckz/fsb/internal/utils"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/gotd/td/tg"
	range_parser "github.com/quantumsheep/range-parser"
	"github.com/speps/go-hashids/v2"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
)

var log *zap.Logger

func (e *allRoutes) LoadHome(r *Route) {
	log = e.log.Named("Stream")
	defer log.Info("Loaded stream routes")

	// روتر قدیمی
	r.Engine.GET("/stream/:messageID", getStreamRoute)

	r.Engine.GET("/:file_id/:filename", getFileByHashRoute)
}

// هندلر روتر قدیمی
func getStreamRoute(ctx *gin.Context) {
	messageIDParm := ctx.Param("messageID")
	messageID, err := strconv.Atoi(messageIDParm)
	if err != nil {
		http.Error(ctx.Writer, err.Error(), http.StatusBadRequest)
		return
	}

	serveTelegramFile(ctx, messageID, "")
}

// هندلر روتر جدید با HashID
func getFileByHashRoute(ctx *gin.Context) {
	hashIDParam := ctx.Param("file_id")
	filenameParam := ctx.Param("filename")

	hd := hashids.NewData()
	hd.Salt = config.ValueOf.HASHSALT
	hd.MinLength = config.ValueOf.HASH_MIN_LEN
	h, err := hashids.NewWithData(hd)
	if err != nil {
		http.Error(ctx.Writer, "Internal Server Error: HashID setup failed", http.StatusInternalServerError)
		return
	}

	// 2. دیکد کردن HashID به عدد
	numbers, err := h.DecodeWithError(hashIDParam)
	if err != nil || len(numbers) == 0 {
		http.Error(ctx.Writer, "Invalid file_id", http.StatusBadRequest)
		return
	}

	messageID := int(numbers[0])

	// 3. ارسال به تابع استریم و درخواست برای بررسی تطابق نام فایل
	serveTelegramFile(ctx, messageID, filenameParam)
}

// تابع مشترک برای استریم فایل
func serveTelegramFile(ctx *gin.Context, messageID int, expectedFilename string) {
	w := ctx.Writer
	r := ctx.Request

	worker := bot.GetNextWorker()
	if worker == nil {
		http.Error(w, "no worker available", http.StatusServiceUnavailable)
		return
	}

	file, err := utils.FileFromMessage(ctx, worker.Client, messageID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// --- بخش جدید: بررسی نام فایل در صورتی که روتر جدید صدا زده شده باشد ---
	if expectedFilename != "" && file.FileName != expectedFilename {
		http.Error(w, "File not found or filename mismatch", http.StatusNotFound)
		return
	}
	// ------------------------------------------------------------------------

	// for photo messages
	if file.FileSize == 0 {
		res, err := worker.Client.API().UploadGetFile(ctx, &tg.UploadGetFileRequest{
			Location: file.Location,
			Offset:   0,
			Limit:    1024 * 1024,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		result, ok := res.(*tg.UploadFile)
		if !ok {
			http.Error(w, "unexpected response", http.StatusInternalServerError)
			return
		}
		fileBytes := result.GetBytes()
		ctx.Header("Content-Disposition", fmt.Sprintf("inline; filename=\"%s\"", file.FileName))
		if r.Method != "HEAD" {
			ctx.Data(http.StatusOK, file.MimeType, fileBytes)
		}
		return
	}

	ctx.Header("Accept-Ranges", "bytes")
	ctx.Header("Cache-Control", "public, max-age=31536000, immutable")

	var start, end int64
	rangeHeader := r.Header.Get("Range")

	status := http.StatusOK
	if rangeHeader == "" {
		start = 0
		end = file.FileSize - 1
	} else {
		ranges, err := range_parser.Parse(file.FileSize, r.Header.Get("Range"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		start = ranges[0].Start
		end = ranges[0].End
		ctx.Header("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, file.FileSize))
		log.Info("Content-Range", zap.Int64("start", start), zap.Int64("end", end), zap.Int64("fileSize", file.FileSize))
		status = http.StatusPartialContent
	}

	contentLength := end - start + 1
	mimeType := file.MimeType

	if mimeType == "" {
		mimeType = "application/octet-stream"
	}

	ctx.Header("Content-Type", mimeType)
	ctx.Header("Content-Length", strconv.FormatInt(contentLength, 10))

	disposition := "inline"

	if ctx.Query("d") == "true" {
		disposition = "attachment"
	}

	ctx.Header("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"", disposition, file.FileName))

	w.WriteHeader(status)

	if r.Method != "HEAD" {
		// نکته: متغیر isProUser در کدهای شما گلوبال فرض شده است
		lr, err := utils.NewTelegramReader(r.Context(), worker.Client, file.Location, start, end, contentLength)
		if err != nil {
			log.Error("Failed to create telegram reader",
				zap.Int("worker_id", worker.ID),
				zap.Error(err),
			)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer lr.Close()

		buf := make([]byte, 32*1024)
		if _, err := io.CopyBuffer(w, lr, buf); err != nil {
			log.Error("Error while copying stream",
				zap.Int("worker_id", worker.ID),
				zap.Error(err),
			)
		}
	}
}
