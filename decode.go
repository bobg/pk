package pk

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"strconv"

	"github.com/pkg/errors"
	"perkeep.org/pkg/blob"
)

// Decoder is an object that can unmarshal data into Go data structures from a Perkeep server.
type Decoder struct {
	src blob.Fetcher
}

// NewDecoder creates a new Decoder reading from src, a Perkeep server.
func NewDecoder(src blob.Fetcher) *Decoder {
	return &Decoder{src: src}
}

var reftype = reflect.TypeOf(blob.Ref{})

// Decode decodes the Perkeep blob or blobs rooted at ref,
// unmarshaling into obj, which must be a non-nil pointer.
// See Unmarshal for more information.
func (d *Decoder) Decode(ctx context.Context, ref blob.Ref, obj interface{}) error {
	if u, ok := obj.(Unmarshaler); ok {
		return u.PkUnmarshal(ctx, d.src, ref)
	}

	v := reflect.ValueOf(obj)
	t := v.Type()
	if t.Kind() != reflect.Ptr {
		return ErrNotPointer
	}
	if v.IsNil() {
		return ErrNilPointer
	}

	r, size, err := d.src.Fetch(ctx, ref)
	if err != nil {
		return errors.Wrapf(err, "fetching %s from src", ref)
	}
	defer r.Close()

	s, err := ioutil.ReadAll(r)
	if err != nil {
		return errors.Wrapf(err, "reading body of %s", ref)
	}

	elTyp := t.Elem()

	switch elTyp.Kind() {
	case reflect.Bool:
		p := obj.(*bool)
		*p = (size > 0)
		return nil

	case reflect.Int:
		n, err := strconv.ParseInt(string(s), 10, 0)
		if err != nil {
			return errors.Wrapf(err, "parsing int from %s", string(s))
		}
		p := obj.(*int)
		*p = int(n)
		return nil

	case reflect.Int8:
		n, err := strconv.ParseInt(string(s), 10, 8)
		if err != nil {
			return errors.Wrapf(err, "parsing int8 from %s", string(s))
		}
		p := obj.(*int8)
		*p = int8(n)
		return nil

	case reflect.Int16:
		n, err := strconv.ParseInt(string(s), 10, 16)
		if err != nil {
			return errors.Wrapf(err, "parsing int16 from %s", string(s))
		}
		p := obj.(*int16)
		*p = int16(n)
		return nil

	case reflect.Int32:
		n, err := strconv.ParseInt(string(s), 10, 32)
		if err != nil {
			return errors.Wrapf(err, "parsing int32 from %s", string(s))
		}
		p := obj.(*int32)
		*p = int32(n)
		return nil

	case reflect.Int64:
		n, err := strconv.ParseInt(string(s), 10, 64)
		if err != nil {
			return errors.Wrapf(err, "parsing int64 from %s", string(s))
		}
		p := obj.(*int64)
		*p = n
		return nil

	case reflect.Uint:
		n, err := strconv.ParseUint(string(s), 10, 0)
		if err != nil {
			return errors.Wrapf(err, "parsing uint from %s", string(s))
		}
		p := obj.(*uint)
		*p = uint(n)
		return nil

	case reflect.Uint8:
		n, err := strconv.ParseUint(string(s), 10, 8)
		if err != nil {
			return errors.Wrapf(err, "parsing uint8 from %s", string(s))
		}
		p := obj.(*uint8)
		*p = uint8(n)
		return nil

	case reflect.Uint16:
		n, err := strconv.ParseUint(string(s), 10, 16)
		if err != nil {
			return errors.Wrapf(err, "parsing uint16 from %s", string(s))
		}
		p := obj.(*uint16)
		*p = uint16(n)
		return nil

	case reflect.Uint32:
		n, err := strconv.ParseUint(string(s), 10, 32)
		if err != nil {
			return errors.Wrapf(err, "parsing uint32 from %s", string(s))
		}
		p := obj.(*uint32)
		*p = uint32(n)
		return nil

	case reflect.Uint64:
		n, err := strconv.ParseUint(string(s), 10, 64)
		if err != nil {
			return errors.Wrapf(err, "parsing uint64 from %s", string(s))
		}
		p := obj.(*uint64)
		*p = n
		return nil

	case reflect.Float32:
		f, err := strconv.ParseFloat(string(s), 32)
		if err != nil {
			return errors.Wrapf(err, "parsing float32 from %s", string(s))
		}
		p := obj.(*float32)
		*p = float32(f)
		return nil

	case reflect.Float64:
		f, err := strconv.ParseFloat(string(s), 64)
		if err != nil {
			return errors.Wrapf(err, "parsing float64 from %s", string(s))
		}
		p := obj.(*float64)
		*p = f
		return nil

	case reflect.Array:
		var refs []blob.Ref
		dec := d.newJSONDecoder(bytes.NewReader(s))
		err := dec.Decode(&refs)
		if err != nil {
			return errors.Wrap(err, "JSON-decoding blobref array")
		}
		arr := v.Elem()
		return d.buildArray(ctx, arr, refs)

	case reflect.Slice:
		var refs []blob.Ref
		dec := d.newJSONDecoder(bytes.NewReader(s))
		err := dec.Decode(&refs)
		if err != nil {
			return errors.Wrap(err, "JSON-decoding blobref slice")
		}
		slice := v.Elem()
		slice, err = d.buildSlice(ctx, slice, refs)
		if err != nil {
			return err
		}
		v.Elem().Set(slice)
		return nil

	case reflect.Map:
		kt := elTyp.Key()
		mt := reflect.MapOf(kt, reftype)
		mm := reflect.New(mt)
		dec := d.newJSONDecoder(bytes.NewReader(s))
		err := dec.Decode(mm.Interface())
		if err != nil {
			return errors.Wrap(err, "JSON-decoding map[K]blob.Ref")
		}
		return d.buildMap(ctx, v.Elem(), mm.Elem())

	case reflect.String:
		p := obj.(*string)
		*p = string(s)
		return nil

	case reflect.Struct:
		// Construct an intermediate struct type for JSON-unmarshaling into.

		var ftypes []reflect.StructField
		for i := 0; i < elTyp.NumField(); i++ {
			tf := elTyp.Field(i)
			name, o := parseTag(tf)
			tf.Tag = reflect.StructTag(fmt.Sprintf(`%s json:"%s"`, tf.Tag, name))
			if o.omit || o.inline {
				ftypes = append(ftypes, tf)
				continue
			}
			if !o.external {
				switch tf.Type.Kind() {
				case reflect.Slice:
					tf.Type = reflect.SliceOf(reftype)
					ftypes = append(ftypes, tf)
					continue

				case reflect.Array:
					tf.Type = reflect.SliceOf(reftype) // sic, not ArrayOf
					ftypes = append(ftypes, tf)
					continue

				case reflect.Map:
					tf.Type = reflect.MapOf(tf.Type.Key(), reftype)
					ftypes = append(ftypes, tf)
					continue
				}
			}
			tf.Type = reftype
			ftypes = append(ftypes, tf)
		}
		intermediateTyp := reflect.StructOf(ftypes)
		intermediateStruct := reflect.New(intermediateTyp)
		dec := d.newJSONDecoder(bytes.NewReader(s))
		err := dec.Decode(intermediateStruct.Interface())
		if err != nil {
			return errors.Wrap(err, "JSON-decoding into intermediate struct")
		}

		structVal := v.Elem()
		for i := 0; i < elTyp.NumField(); i++ {
			tf := elTyp.Field(i)
			name, o := parseTag(tf)
			if o.omit {
				continue
			}
			field := structVal.Field(i)
			ifield := intermediateStruct.Elem().Field(i)
			if o.inline {
				field.Set(ifield)
				continue
			}
			if !o.external {
				switch tf.Type.Kind() {
				case reflect.Slice:
					refs := ifield.Interface().([]blob.Ref)
					slice, err := d.buildSlice(ctx, field, refs)
					if err != nil {
						return errors.Wrapf(err, "building slice for field %s", name)
					}
					field.Set(slice)
					continue

				case reflect.Array:
					refs := ifield.Interface().([]blob.Ref)
					err = d.buildArray(ctx, field, refs)
					if err != nil {
						return errors.Wrapf(err, "building array for field %s", name)
					}
					continue

				case reflect.Map:
					err = d.buildMap(ctx, field, ifield)
					if err != nil {
						return errors.Wrapf(err, "building map for field %s", name)
					}
					continue
				}
			}
			if ifield.IsZero() {
				continue
			}
			fieldRef := ifield.Interface().(blob.Ref)
			newFieldVal := reflect.New(tf.Type)
			err = d.Decode(ctx, fieldRef, newFieldVal.Interface())
			if err != nil {
				return errors.Wrapf(err, "decoding ref %s for field %s", fieldRef, name)
			}
			field.Set(newFieldVal.Elem())
		}
		return nil

	case reflect.Ptr:
		ptr := v.Elem()
		if ptr.IsNil() {
			newItem := reflect.New(elTyp.Elem())
			v.Elem().Set(newItem)
		}
		// Recursively unmarshal into the thing ptr points to.
		return d.Decode(ctx, ref, ptr.Interface())

	default:
		return ErrUnsupportedType{Name: t.Name()}
	}
}

func (d *Decoder) newJSONDecoder(r io.Reader) *json.Decoder {
	result := json.NewDecoder(r)
	result.UseNumber()
	return result
}

func (d *Decoder) buildSlice(ctx context.Context, slice reflect.Value, refs []blob.Ref) (reflect.Value, error) {
	slice.SetLen(0)
	elTyp := slice.Type().Elem()
	for _, ref := range refs {
		elVal := reflect.New(elTyp)
		err := d.Decode(ctx, ref, elVal.Interface())
		if err != nil {
			return reflect.Value{}, err
		}
		slice = reflect.Append(slice, elVal.Elem())
	}
	return slice, nil
}

func (d *Decoder) buildArray(ctx context.Context, arr reflect.Value, refs []blob.Ref) error {
	elTyp := arr.Type().Elem()
	zero := reflect.Zero(elTyp)
	for i := 0; i < arr.Len(); i++ {
		el := arr.Index(i)
		el.Set(zero)
		if i < len(refs) {
			err := d.Decode(ctx, refs[i], el.Addr().Interface())
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// dst is a map[K]T
// refs is a map[K]blob.Ref
func (d *Decoder) buildMap(ctx context.Context, dst, refs reflect.Value) error {
	dstTyp := dst.Type()
	if dst.IsNil() {
		dst.Set(reflect.MakeMap(dstTyp))
	}
	iter := refs.MapRange()
	for iter.Next() {
		k := iter.Key()
		ref := iter.Value().Interface().(blob.Ref)
		item := reflect.New(dstTyp.Elem())
		err := d.Decode(ctx, ref, item.Interface())
		if err != nil {
			return err
		}
		dst.SetMapIndex(k, item.Elem())
	}
	return nil
}
