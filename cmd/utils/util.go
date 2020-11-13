package utils

import (
	"io"
	"net/http"
	"os"

	"github.com/ethereum/go-ethereum/log"
)

func downloadBackup(fn string) (string, error) {
	log.Info("Download export file", "url", fn)
	resp, err := http.Get(fn)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	// Create the file
	fn = "export.backup"
	out, err := os.Create(fn)
	if err != nil {
		return "", err
	}
	defer out.Close()

	// Write the body to file
	_, err = io.Copy(out, resp.Body)
	return fn, err
}
