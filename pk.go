// Package pk supports marshaling and unmarshaling Go data structures as Perkeep objects.
package pk

import (
	"context"
	"errors"
	"fmt"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
)

// Marshaler is the type of an object that knows how to store itself in Perkeep.
type Marshaler interface {
	PkMarshal(context.Context, blobserver.BlobReceiver) (blob.Ref, error)
}

// Unmarshaler is the type of an object that knows how to populate itself from Perkeep.
type Unmarshaler interface {
	PkUnmarshal(context.Context, blob.Fetcher, blob.Ref) error
}

// Marshal stores obj to dst as a tree of Perkeep blobs.
// It returns a reference to the root of the tree.
//
// How obj is marshaled depends on its type.
//
// Boolean false marshals as the zero-byte blob.
// Boolean true marshals as the four-byte string "true".
// (When unmarshaling, all blobs other than the zero-byte blob count as true.)
//
// Integers and floats of all sizes are marshaled as human-readable base 10 number strings.
//
// Arrays and slices are marshaled as a JSON array of blobrefs: "[ref,ref,...]".
// The blobrefs are those of the recursively marshaled members of the array or slice.
//
// A map of type map[K]T is marshaled as the JSON encoding of a map[K]blob.Ref.
// The blobrefs are those of the recursively marshaled values of the map.
// (The keys of the map are not marshaled, however.)
//
// A string is marshaled as a blob equal to the bytes of the string.
//
// (xxx describe structs)
func Marshal(ctx context.Context, dst blobserver.BlobReceiver, obj interface{}) (blob.Ref, error) {
	return NewEncoder(dst).Encode(ctx, obj)
}

// Unmarshal populates obj from the tree of blobs in src rooted at ref.
// Unmarshaling is the inverse of marshaling.
// See Marshal for the rules of how Go types correspond to marshaled Perkeep blobs.
func Unmarshal(ctx context.Context, src blob.Fetcher, ref blob.Ref, obj interface{}) error {
	return NewDecoder(src).Decode(ctx, ref, obj)
}

// ErrUnsupportedType indicates an attempt to marshal or unmarshal an unsupported Go type.
type ErrUnsupportedType struct {
	Name string
}

// Error implements the error interface.
func (e ErrUnsupportedType) Error() string {
	if e.Name == "" {
		return "unsupported type"
	}
	return fmt.Sprintf("unsupported type \"%s\"", e.Name)
}

var (
	// ErrDecoding is produced when a blob can't be unmarshaled into a given Go object.
	ErrDecoding = errors.New("decoding")

	// ErrNotPointer is produced when a non-pointer is passed to Unmarshal or Decode.
	ErrNotPointer = errors.New("not pointer")

	// ErrNilPointer is produced when a nil pointer is passed to Unmarshal or Decode.
	ErrNilPointer = errors.New("nil pointer")
)
