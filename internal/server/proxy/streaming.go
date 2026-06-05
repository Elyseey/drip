package proxy

import (
	"io"
	"net/http"

	"drip/internal/shared/pool"
)

func flushResponse(w http.ResponseWriter) {
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

func copyResponseBodyFlushing(w http.ResponseWriter, body io.Reader) (int64, error) {
	bufPtr := pool.GetBuffer(pool.SizeSmall)
	defer pool.PutBuffer(bufPtr)

	buf := (*bufPtr)[:pool.SizeSmall]
	var written int64

	for {
		nr, er := body.Read(buf)
		if nr > 0 {
			nw, ew := w.Write(buf[:nr])
			if nw > 0 {
				written += int64(nw)
				flushResponse(w)
			}
			if ew != nil {
				return written, ew
			}
			if nr != nw {
				return written, io.ErrShortWrite
			}
		}
		if er != nil {
			if er == io.EOF {
				return written, nil
			}
			return written, er
		}
	}
}
