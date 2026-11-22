package protocol

import (
	"bufio"
	"strings"
	"testing"
)

func TestValidateHandshakeResponse(t *testing.T) {
	const key = "dGhlIHNhbXBsZSBub25jZQ=="
	const accept = "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="

	t.Run("accepts valid RFC 6455 response", func(t *testing.T) {
		headers := "Upgrade: websocket\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n"
		if err := validateHandshakeResponse("HTTP/1.1 101 Switching Protocols\r\n", bufio.NewReader(strings.NewReader(headers)), key); err != nil {
			t.Fatalf("valid handshake rejected: %v", err)
		}
	})

	t.Run("rejects incorrect accept key", func(t *testing.T) {
		headers := "Sec-WebSocket-Accept: invalid\r\n\r\n"
		if err := validateHandshakeResponse("HTTP/1.1 101 Switching Protocols\r\n", bufio.NewReader(strings.NewReader(headers)), key); err == nil {
			t.Fatal("expected invalid accept key to be rejected")
		}
	})

	t.Run("rejects non-upgrade status", func(t *testing.T) {
		if err := validateHandshakeResponse("HTTP/1.1 200 OK\r\n", bufio.NewReader(strings.NewReader("\r\n")), key); err == nil {
			t.Fatal("expected non-101 status to be rejected")
		}
	})
}

func TestEncodeWSFrameUsesValidMask(t *testing.T) {
	payload := []byte("turbostorm-ping")
	frame, err := encodeWSFrame(0x1, payload)
	if err != nil {
		t.Fatalf("encode frame: %v", err)
	}
	if len(frame) != 6+len(payload) {
		t.Fatalf("unexpected frame length: got %d", len(frame))
	}
	if frame[0] != 0x81 {
		t.Fatalf("expected FIN text frame, got %#x", frame[0])
	}
	if frame[1]&0x80 == 0 {
		t.Fatal("client frame must set the mask bit")
	}
	mask := frame[2:6]
	for i, b := range payload {
		if frame[6+i] != b^mask[i%4] {
			t.Fatalf("payload byte %d was not masked correctly", i)
		}
	}
}

func TestComputeWSKeyAcceptRFCExample(t *testing.T) {
	got := computeWSKeyAccept("dGhlIHNhbXBsZSBub25jZQ==")
	want := "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
	if got != want {
		t.Fatalf("unexpected accept key: got %q, want %q", got, want)
	}
}
