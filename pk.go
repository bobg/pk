// Package pk supports marshaling and unmarshaling Go data structures as Perkeep objects.
package pk

import (
	"context"
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
// xxx describe how different types are marshaled.
func Marshal(ctx context.Context, dst blobserver.BlobReceiver, obj interface{}) (blob.Ref, error) {
	return NewEncoder(dst).Encode(ctx, obj)
}

// Unmarshal populates obj from the tree of blobs in src rooted at ref.
// xxx describe how different types are unmarshaled.
func Unmarshal(ctx context.Context, src blob.Fetcher, ref blob.Ref, obj interface{}) error {
	return NewDecoder(src).Decode(ctx, ref, obj)
}

type ErrUnsupportedType struct {
	Name string
}

func (e ErrUnsupportedType) Error() string {
	if e.Name == "" {
		return "unsupported type"
	}
	return fmt.Sprintf("unsupported type \"%s\"", e.Name)
}
