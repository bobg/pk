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
		return ErrNotPointer
	}
	if v.IsNil() {
		return ErrNilPointer
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
		arr := v.Elem()
		return d.buildArray(ctx, arr, refs)

	case reflect.Slice:
		var refs []blob.Ref
		dec := d.newJSONDecoder(bytes.NewReader(s))
		err := dec.Decode(&refs)
		if err != nil {
			return err
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
		mt := reflect.MapOf(kt, reflect.TypeOf(blob.Ref{}))
		mm := reflect.New(mt)
		dec := d.newJSONDecoder(bytes.NewReader(s))
		err := dec.Decode(mm.Interface())
		if err != nil {
			return err
		}
		return d.buildMap(ctx, v.Elem(), mm.Elem())

	case reflect.String:
		p := obj.(*string)
		*p = string(s)
		return nil

	case reflect.Struct:
		var m map[string]interface{}
		dec := d.newJSONDecoder(bytes.NewReader(s))
		err := dec.Decode(&m)
		if err != nil {
			return err
		}

		structVal := v.Elem()

		for i := 0; i < elTyp.NumField(); i++ {
			tf := elTyp.Field(i)
			name, o := parseTag(tf)
			if o.omit {
				continue
			}

			mItem, ok := m[name]
			if !ok {
				continue
			}

			if o.inline {
				// m[name] is the result of JSON-marshaling, then JSON-unmarshaling the original field value.
				// That means it could be a string, a number, a map, a slice, etc.
				// Re-JSON-marshaling it should allow us to un-JSON-marshal it into an object of the right type.
				// This is the simplest but probably not the most efficient approach to take here.
				reJSON, err := json.Marshal(mItem)
				if err != nil {
					return err
				}
				dec := d.newJSONDecoder(bytes.NewReader(reJSON))
				fieldVal := reflect.New(tf.Type)
				err = dec.Decode(fieldVal.Interface())
				if err != nil {
					return err
				}
				structVal.Field(i).Set(fieldVal)
				continue
			}
			if !o.external {
				switch tf.Type.Kind() {
				case reflect.Slice:
					refs, ok := mItem.([]blob.Ref)
					if !ok {
						return ErrDecoding
					}
					slice := structVal.Field(i)
					slice, err = d.buildSlice(ctx, slice, refs)
					if err != nil {
						return err
					}
					structVal.Field(i).Set(slice)
					continue

				case reflect.Array:
					refs, ok := mItem.([]blob.Ref)
					if !ok {
						return ErrDecoding
					}
					arr := structVal.Field(i)
					err = d.buildArray(ctx, arr, refs)
					if err != nil {
						return err
					}
					continue

				case reflect.Map:
					// Re-JSON-marshal m[name] and unmarshal it into a map[K]blob.Ref.
					reJSON, err := json.Marshal(mItem)
					if err != nil {
						return err
					}
					mt := reflect.MapOf(tf.Type.Key(), reflect.TypeOf(blob.Ref{}))
					refsMap := reflect.New(mt)
					dec := d.newJSONDecoder(bytes.NewReader(reJSON))
					err = dec.Decode(refsMap.Interface())
					if err != nil {
						return err
					}
					err = d.buildMap(ctx, structVal.Field(i), refsMap.Elem())
					if err != nil {
						return err
					}
					continue
				}
			}

			fieldRef, ok := mItem.(blob.Ref)
			if !ok {
				return ErrDecoding
			}
			fieldVal := reflect.New(tf.Type)
			err = d.Decode(ctx, fieldRef, fieldVal.Interface())
			if err != nil {
				return err
			}
			structVal.Field(i).Set(fieldVal)
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
