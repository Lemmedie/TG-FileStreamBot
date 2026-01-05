package utils

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/celestix/gotgproto"
	"github.com/gotd/td/tg"
	"go.uber.org/zap"
)

type telegramReader struct {
	ctx           context.Context
	log           *zap.Logger
	client        *gotgproto.Client
	location      tg.InputFileLocationClass
	start         int64
	end           int64
	next          func() ([]byte, error)
	buffer        []byte
	bytesread     int64
	chunkSize     int64
	i             int
	contentLength int64
	// prefetching
	chunkChan chan []byte
	errChan   chan error
	cancel    context.CancelFunc
	mu        sync.Mutex
}

func (*telegramReader) Close() error {
	return nil
}

func NewTelegramReader(
	ctx context.Context,
	client *gotgproto.Client,
	location tg.InputFileLocationClass,
	start int64,
	end int64,
	contentLength int64,
	isProUser bool,
) (io.ReadCloser, error) {

	chunkSize := int64(1024 * 1024)
	/* if isProUser {
		chunk_size = int64(64 * 1024)
	} */

	cctx, cancel := context.WithCancel(ctx)
	r := &telegramReader{
		ctx:           cctx,
		log:           Logger.Named("telegramReader"),
		location:      location,
		client:        client,
		start:         start,
		end:           end,
		chunkSize:     chunkSize,
		contentLength: contentLength,
		chunkChan:     make(chan []byte, 2),
		errChan:       make(chan error, 1),
		cancel:        cancel,
	}
	r.log.Sugar().Debug("Start")
	r.next = r.partStream()

	// start background prefetcher
	go func() {
		defer close(r.chunkChan)
		for {
			select {
			case <-r.ctx.Done():
				return
			default:
			}
			b, err := r.next()
			if err != nil {
				select {
				case r.errChan <- err:
				default:
				}
				return
			}
			if len(b) == 0 {
				return
			}
			select {
			case r.chunkChan <- b:
			case <-r.ctx.Done():
				return
			}
		}
	}()

	return r, nil
}

func (r *telegramReader) Read(p []byte) (n int, err error) {
	if r.contentLength > 0 && r.bytesread >= r.contentLength {
		r.log.Sugar().Debug("EOF (bytesread >= contentLength)")
		return 0, io.EOF
	}

	if r.i >= len(r.buffer) {
		// try to read from prefetch channel or error channel
		select {
		case b, ok := <-r.chunkChan:
			if !ok {
				return 0, io.EOF
			}
			r.buffer = b
			r.log.Debug("Next Buffer (prefetched)", zap.Int("len", len(r.buffer)))
			r.i = 0
		case err := <-r.errChan:
			return 0, err
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}
	// Don't read past the declared content length if known
	toCopy := len(p)
	if r.contentLength > 0 {
		remaining := r.contentLength - r.bytesread
		if remaining <= 0 {
			return 0, io.EOF
		}
		if int64(toCopy) > remaining {
			toCopy = int(remaining)
		}
	}

	// also don't copy more than what's in the buffer
	avail := len(r.buffer) - r.i
	if toCopy > avail {
		toCopy = avail
	}

	n = copy(p, r.buffer[r.i:r.i+toCopy])
	r.i += n
	r.bytesread += int64(n)
	return n, nil
}

func (r *telegramReader) chunk(offset int64, limit int64) ([]byte, error) {

	req := &tg.UploadGetFileRequest{
		Offset:   offset,
		Limit:    int(limit),
		Location: r.location,
	}

	res, err := r.client.API().UploadGetFile(r.ctx, req)

	if err != nil {
		return nil, err
	}

	switch result := res.(type) {
	case *tg.UploadFile:
		return result.Bytes, nil
	default:
		return nil, fmt.Errorf("unexpected type %T", result)
	}
}

func (r *telegramReader) partStream() func() ([]byte, error) {

	start := r.start
	end := r.end
	offset := start - (start % r.chunkSize)

	firstPartCut := int(start - offset)
	lastPartCut := int((end % r.chunkSize) + 1)
	partCount := int((end - offset + r.chunkSize) / r.chunkSize)
	currentPart := 1

	readData := func() ([]byte, error) {
		if currentPart > partCount {
			return make([]byte, 0), nil
		}
		res, err := r.chunk(offset, r.chunkSize)
		if err != nil {
			return nil, err
		}
		if len(res) == 0 {
			return res, nil
		} else if partCount == 1 {
			res = res[firstPartCut:lastPartCut]
		} else if currentPart == 1 {
			res = res[firstPartCut:]
		} else if currentPart == partCount {
			res = res[:lastPartCut]
		}

		r.log.Sugar().Debugf("Part %d/%d", currentPart, partCount)
		currentPart++
		offset += r.chunkSize
		return res, nil
	}
	return readData
}
