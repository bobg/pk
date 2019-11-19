package pk

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"reflect"
	"strconv"

	"github.com/pkg/errors"
	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver"
)

type Encoder struct {
	dst blobserver.BlobReceiver

	// These are passed along to a json.Encoder.
	escapeHTML     bool
	prefix, indent string

	// TODO: an option to write proper schema blobs
	// (with a callback for determining each item's camliType).
}

func NewEncoder(dst blobserver.BlobReceiver) *Encoder {
	return &Encoder{dst: dst}
}

func (e *Encoder) SetEscapeHTML(val bool) {
	e.escapeHTML = val
}

func (e *Encoder) SetIndent(prefix, indent string) {
	e.prefix, e.indent = prefix, indent
}

func (e *Encoder) Encode(ctx context.Context, obj interface{}) (blob.Ref, error) {
	if m, ok := obj.(Marshaler); ok {
		return m.PkMarshal(ctx, e.dst)
	}

	var (
		v = reflect.ValueOf(obj)
		t = v.Type()
		k = t.Kind()
	)

	// Dereference pointers, pointers to pointers, etc.
	for k == reflect.Ptr && !v.IsNil() {
		v = v.Elem()
		t = v.Type()
		k = t.Kind()
	}

	switch k {
	case reflect.Map, reflect.Slice:
		if v.IsNil() {
			sref, err := blobserver.ReceiveString(ctx, e.dst, "")
			return sref.Ref, err
		}
	}

	switch k {
	case reflect.Bool:
		// The empty blob is false, all other blobs are true.
		var s string
		if v.Bool() {
			s = "true"
		}
		sref, err := blobserver.ReceiveString(ctx, e.dst, s)
		return sref.Ref, errors.Wrap(err, "storing bool val")

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		s := strconv.FormatInt(v.Int(), 10)
		sref, err := blobserver.ReceiveString(ctx, e.dst, s)
		return sref.Ref, errors.Wrap(err, "storing int val")

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		s := strconv.FormatUint(v.Uint(), 10)
		sref, err := blobserver.ReceiveString(ctx, e.dst, s)
		return sref.Ref, errors.Wrap(err, "storing int val")

	case reflect.Float32:
		s := strconv.FormatFloat(v.Float(), 'f', -1, 32)
		sref, err := blobserver.ReceiveString(ctx, e.dst, s)
		return sref.Ref, errors.Wrap(err, "storing float32 val")

	case reflect.Float64:
		s := strconv.FormatFloat(v.Float(), 'f', -1, 64)
		sref, err := blobserver.ReceiveString(ctx, e.dst, s)
		return sref.Ref, errors.Wrap(err, "storing float64 val")

	case reflect.Array, reflect.Slice:
		refs, err := e.encodeSliceOrArray(ctx, v)
		if err != nil {
			return blob.Ref{}, err
		}
		buf := new(bytes.Buffer)
		enc := e.newJSONEncoder(buf)
		err = enc.Encode(refs)
		if err != nil {
			return blob.Ref{}, err
		}
		sref, err := blobserver.ReceiveString(ctx, e.dst, buf.String())
		return sref.Ref, err

	case reflect.Map:
		// Keys are not blobrefs. (Should they be?)
		mm, err := e.encodeMap(ctx, v)
		if err != nil {
			return blob.Ref{}, err
		}
		buf := new(bytes.Buffer)
		enc := e.newJSONEncoder(buf)
		err = enc.Encode(mm.Interface())
		if err != nil {
			return blob.Ref{}, err
		}
		sref, err := blobserver.ReceiveString(ctx, e.dst, buf.String())
		return sref.Ref, err

	case reflect.String:
		sref, err := blobserver.ReceiveString(ctx, e.dst, obj.(string))
		return sref.Ref, errors.Wrap(err, "storing string")

	case reflect.Struct:
		m := make(map[string]interface{})
		for i := 0; i < v.NumField(); i++ {
			tf := t.Field(i)
			name, o := parseTag(tf)
			if o.omit {
				continue
			}
			vf := v.Field(i)
			if o.omitEmpty && vf.IsZero() {
				continue
			}
			if o.inline {
				m[name] = vf.Interface()
				continue
			}

			if !o.external {
				// With o.external false (the default),
				// slices and arrays are encoded as [blobref, blobref, ...]
				// and maps are encoded as {key: blobref, key: blobref, ...}
				//
				// With o.external true, the whole slice/array/map becomes a blobref,
				// like other kinds of value.

				switch tf.Type.Kind() {
				case reflect.Slice, reflect.Array:
					refs, err := e.encodeSliceOrArray(ctx, vf)
					if err != nil {
						return blob.Ref{}, err
					}
					m[name] = refs
					continue

				case reflect.Map:
					mm, err := e.encodeMap(ctx, vf)
					if err != nil {
						return blob.Ref{}, err
					}
					m[name] = mm.Interface()
					continue
				}
			}

			fieldRef, err := e.Encode(ctx, vf.Interface())
			if err != nil {
				return blob.Ref{}, errors.Wrapf(err, "storing field %s of struct type %s", name, t.Name())
			}
			m[name] = fieldRef
		}

		buf := new(bytes.Buffer)
		enc := e.newJSONEncoder(buf)
		err := enc.Encode(m)
		if err != nil {
			return blob.Ref{}, errors.Wrapf(err, "encoding fields of struct type %s", t.Name())
		}

		sref, err := blobserver.ReceiveString(ctx, e.dst, buf.String())
		return sref.Ref, errors.Wrapf(err, "storing struct type %s", t.Name())

	default:
		return blob.Ref{}, ErrUnsupportedType{Name: t.Name()}
	}
}

func (e *Encoder) newJSONEncoder(w io.Writer) *json.Encoder {
	result := json.NewEncoder(w)
	result.SetEscapeHTML(e.escapeHTML)
	result.SetIndent(e.prefix, e.indent)
	return result
}

func (e *Encoder) encodeSliceOrArray(ctx context.Context, sliceOrArray reflect.Value) ([]blob.Ref, error) {
	var refs []blob.Ref
	for i := 0; i < sliceOrArray.Len(); i++ {
		el := sliceOrArray.Index(i)
		ref, err := e.Encode(ctx, el.Interface())
		if err != nil {
			return nil, err // xxx return the refs created so far?
		}
		refs = append(refs, ref)
	}
	return refs, nil
}

// Returns a reflect.Value containing a map[K]blob.Ref, where K is the key type of m.
func (e *Encoder) encodeMap(ctx context.Context, m reflect.Value) (reflect.Value, error) {
	kt := m.Type().Key()
	mt := reflect.MapOf(kt, reflect.TypeOf(blob.Ref{}))
	mm := reflect.MakeMap(mt)
	iter := m.MapRange()
	for iter.Next() {
		mk := iter.Key()
		mv := iter.Value()
		ref, err := e.Encode(ctx, mv.Interface())
		if err != nil {
			return reflect.Value{}, err
		}
		mm.SetMapIndex(mk, reflect.ValueOf(ref))
	}
	return mm, nil
}
