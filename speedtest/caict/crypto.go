// Package caict implements the network speed test protocol of the CAICT
// Tai'er Lab speed test app (com.cnspeedtest.globalspeed), reverse
// engineered from the decrypted iOS binary (v4.4.9) and the Android
// libGSCore.so native core.
//
// Nodes issue a per-test UUID through GET /speed/dovalid with an empty key
// and a locally computed token (see ComputeToken). Downloads use
// /speed/File(1G).dl?r={unix}&key={uuid}; uploads use /speed/doAnalsLoad.do
// with the UUID in the Key header. POST /speed/dovalid?key={uuid} ends the
// session. A rejection alone does not identify its server-side cause.
//
// Local mocks exercise client behavior; they do not establish compatibility
// with every deployed node.
package caict

import (
	"bytes"
	"crypto/des"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// serverListKey is the hardcoded DES key embedded in libGSCore / TaierSpeed
// (visible as a plain string in both binaries). The bundled
// serverlist_encrypt.json is hex-encoded DES-ECB ciphertext of the node list.
const serverListKey = "dw!@#$%^"

// decryptServerList decrypts the hex-encoded DES-ECB serverlist_encrypt.json
// payload shipped inside the app bundles.
func decryptServerList(hexPayload string) ([]byte, error) {
	raw, err := hex.DecodeString(strings.TrimSpace(hexPayload))
	if err != nil {
		return nil, fmt.Errorf("caict: hex decode serverlist: %w", err)
	}
	plain, err := desECBDecryptRaw([]byte(serverListKey), raw)
	if err != nil {
		return nil, err
	}
	return unpadPKCS7(plain)
}

// desECBDecryptRaw decrypts data with DES in ECB mode, block by block, the
// way the upstream engine iterates (input length must be a multiple of the
// block size). No padding is stripped.
func desECBDecryptRaw(key, data []byte) ([]byte, error) {
	if len(key) != 8 {
		return nil, errors.New("caict: DES key must be 8 bytes")
	}
	if len(data) == 0 || len(data)%des.BlockSize != 0 {
		return nil, errors.New("caict: ciphertext length is not a multiple of the DES block size")
	}
	block, err := des.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("caict: des.NewCipher: %w", err)
	}
	out := make([]byte, len(data))
	for off := 0; off < len(data); off += des.BlockSize {
		block.Decrypt(out[off:off+des.BlockSize], data[off:off+des.BlockSize])
	}
	return out, nil
}

func unpadPKCS7(b []byte) ([]byte, error) {
	if len(b) == 0 {
		return nil, errors.New("caict: empty plaintext")
	}
	n := int(b[len(b)-1])
	if n == 0 || n > des.BlockSize || n > len(b) {
		return nil, errors.New("caict: invalid PKCS7 padding")
	}
	for _, c := range b[len(b)-n:] {
		if int(c) != n {
			return nil, errors.New("caict: corrupted PKCS7 padding")
		}
	}
	return b[:len(b)-n], nil
}

// encryptServerList is the inverse of decryptServerList: it PKCS7-pads the
// plaintext, encrypts it with DES-ECB and hex-encodes the result in the
// bundle's serverlist_encrypt.json format. It is kept for regenerating the
// embedded node table.
//
//nolint:unused // regeneration helper for serverlist_encrypt.json
func encryptServerList(plain []byte) (string, error) {
	block, err := des.NewCipher([]byte(serverListKey))
	if err != nil {
		return "", fmt.Errorf("caict: des.NewCipher: %w", err)
	}
	pad := des.BlockSize - len(plain)%des.BlockSize
	padded := append(append([]byte{}, plain...), bytes.Repeat([]byte{byte(pad)}, pad)...)
	out := make([]byte, len(padded))
	for off := 0; off < len(padded); off += des.BlockSize {
		block.Encrypt(out[off:off+des.BlockSize], padded[off:off+des.BlockSize])
	}
	return hex.EncodeToString(out), nil
}
