package pk

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"reflect"
	"strconv"

	"perkeep.org/pkg/blob"
)

type Decoder struct {
	src blob.Fetcher
}

func NewDecoder(src blob.Fetcher) *Decoder {
	return &Decoder{src: src}
}

func (d *Decoder) Decode(ctx context.Context, ref blob.Ref, obj interface{}) error {
	if u, ok := obj.(Unmarshaler); ok {
		return u.PkUnmarshal(ctx, d.src, ref)
	}

	v := reflect.ValueOf(obj)
	t := v.Type()
	if t.Kind() != reflect.Ptr {
		// xxx err
	}

	r, size, err := d.src.Fetch(ctx, ref)
	if err != nil {
		return err
	}
	defer r.Close()

	s, err := ioutil.ReadAll(r)
	if err != nil {
		return err
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
			return err
		}
		p := obj.(*int)
		*p = int(n)
		return nil

	case reflect.Int8:
		n, err := strconv.ParseInt(string(s), 10, 8)
		if err != nil {
			return err
		}
		p := obj.(*int8)
		*p = int8(n)
		return nil

	case reflect.Int16:
		n, err := strconv.ParseInt(string(s), 10, 16)
		if err != nil {
			return err
		}
		p := obj.(*int16)
		*p = int16(n)
		return nil

	case reflect.Int32:
		n, err := strconv.ParseInt(string(s), 10, 32)
		if err != nil {
			return err
		}
		p := obj.(*int32)
		*p = int32(n)
		return nil

	case reflect.Int64:
		n, err := strconv.ParseInt(string(s), 10, 64)
		if err != nil {
			return err
		}
		p := obj.(*int64)
		*p = n
		return nil

	case reflect.Uint:
		n, err := strconv.ParseUint(string(s), 10, 0)
		if err != nil {
			return err
		}
		p := obj.(*uint)
		*p = uint(n)
		return nil

	case reflect.Uint8:
		n, err := strconv.ParseUint(string(s), 10, 8)
		if err != nil {
			return err
		}
		p := obj.(*uint8)
		*p = uint8(n)
		return nil

	case reflect.Uint16:
		n, err := strconv.ParseUint(string(s), 10, 16)
		if err != nil {
			return err
		}
		p := obj.(*uint16)
		*p = uint16(n)
		return nil

	case reflect.Uint32:
		n, err := strconv.ParseUint(string(s), 10, 32)
		if err != nil {
			return err
		}
		p := obj.(*uint32)
		*p = uint32(n)
		return nil

	case reflect.Uint64:
		n, err := strconv.ParseUint(string(s), 10, 64)
		if err != nil {
			return err
		}
		p := obj.(*uint64)
		*p = n
		return nil

	case reflect.Float32:
		f, err := strconv.ParseFloat(string(s), 32)
		if err != nil {
			return err
		}
		p := obj.(*float32)
		*p = float32(f)
		return nil

	case reflect.Float64:
		f, err := strconv.ParseFloat(string(s), 64)
		if err != nil {
			return err
		}
		p := obj.(*float64)
		*p = f
		return nil

	case reflect.Array:
		var refs []blob.Ref
		dec := d.newJSONDecoder(bytes.NewReader(s))
		err := dec.Decode(&refs)
		if err != nil {
			return err
		}

		var (
			arr      = v.Elem()
			arrElTyp = elTyp.Elem()
		)
		for i := 0; i < arr.Len(); i++ {
			el := arr.Index(i)
			el.Set(reflect.Zero(arrElTyp))
			if i < len(refs) {
				err = d.Decode(ctx, refs[i], el.Addr().Interface())
				if err != nil {
					return err
				}
			}
		}
		return nil

	case reflect.Slice:
		var refs []blob.Ref
		dec := d.newJSONDecoder(bytes.NewReader(s))
		err := dec.Decode(&refs)
		if err != nil {
			return err
		}

		var (
			slice      = v.Elem()
			sliceElTyp = elTyp.Elem()
		)
		slice.SetLen(0) // like encoding/json
		for _, elRef := range refs {
			elVal := reflect.New(sliceElTyp)
			err = d.Decode(ctx, elRef, elVal.Interface())
			if err != nil {
				return err
			}
			slice = reflect.Append(slice, elVal.Elem())
		}
		v.Elem().Set(slice)

		return nil

	case reflect.Map:
		kt := elTyp.Key()
		mt := reflect.MapOf(kt, reflect.TypeOf(blob.Ref{}))
		mm := reflect.New(mt)
		dec := d.newJSONDecoder(bytes.NewReader(s))
		err := dec.Decode(mm.Interface())
		if err != nil {
			return err
		}

		dstMap := v.Elem()
		if dstMap.IsNil() {
			dstMap.Set(reflect.MakeMap(elTyp))
		}
		iter := mm.Elem().MapRange()
		for iter.Next() {
			mmk := iter.Key()
			mmv := iter.Value().Interface().(blob.Ref)
			item := reflect.New(elTyp.Elem())
			err = d.Decode(ctx, mmv, item.Interface())
			if err != nil {
				return err
			}
			dstMap.SetMapIndex(mmk, item.Elem())
		}
		return nil

	case reflect.String:
		p := obj.(*string)
		*p = string(s)
		return nil

	case reflect.Struct:
	default:
		return ErrUnsupportedType{Name: t.Name()}
	}

	panic("xxx")
}

func (d *Decoder) newJSONDecoder(r io.Reader) *json.Decoder {
	result := json.NewDecoder(r)
	result.UseNumber()
	return result
}
