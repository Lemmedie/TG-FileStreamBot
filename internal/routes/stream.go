package routes

import (
	"EverythingSuckz/fsb/internal/bot"
	"EverythingSuckz/fsb/internal/utils"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/gotd/td/tg"
	range_parser "github.com/quantumsheep/range-parser"
	"go.uber.org/zap"

	"github.com/gin-gonic/gin"
)

var log *zap.Logger

func (e *allRoutes) LoadHome(r *Route) {
	log = e.log.Named("Stream")
	defer log.Info("Loaded stream route")
	r.Engine.GET("/stream/:messageID", getStreamRoute)
}

func getStreamRoute(ctx *gin.Context) {
	w := ctx.Writer
	r := ctx.Request

	messageIDParm := ctx.Param("messageID")
	messageID, err := strconv.Atoi(messageIDParm)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	/* authHash := ctx.Query("hash")
	if authHash == "" {
		http.Error(w, "missing hash param", http.StatusBadRequest)
		return
	} */

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

	/* expectedHash := utils.PackFile(
		file.FileName,
		file.FileSize,
		file.MimeType,
		file.ID,
	)
	if !utils.CheckHash(authHash, expectedHash) {
		http.Error(w, "invalid hash", http.StatusBadRequest)
		return
	} */

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

	isProUser := false
	if ctx.Query("isProUser") == "true" {
		isProUser = true
	}

	ctx.Header("Content-Disposition", fmt.Sprintf("%s; filename=\"%s\"", disposition, file.FileName))

	// write status after headers are set so client receives correct metadata
	w.WriteHeader(status)

	if r.Method != "HEAD" {
		lr, err := utils.NewTelegramReader(r.Context(), worker.Client, file.Location, start, end, contentLength, isProUser)
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
