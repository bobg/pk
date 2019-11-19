package pk

import (
	"context"
	"io/ioutil"
	"log"
	"reflect"
	"testing"

	"perkeep.org/pkg/blob"
	"perkeep.org/pkg/blobserver/memory"
)

func TestPk(t *testing.T) {
	cases := []struct {
		name  string
		obj   interface{}
		check func(t *testing.T, got, want interface{})
	}{
		{name: "boolean false", obj: false},
		{name: "boolean true", obj: true},
		{name: "int32", obj: int32(17)},
		{name: "empty string", obj: ""},
		{name: "non-empty string", obj: "foo"},
		{name: "slice of strings", obj: []string{"foo", "bar", "baz"}},
		{name: "array of ints", obj: [...]int{10, 11, 12}},
		{name: "map of string to int", obj: map[string]int{"foo": 1, "bar": 2}},
		{
			name: "struct",
			obj: &astruct{
				A: 1,
				B: 2,
				C: "hello",
				D: []string{"foo", "bar"},
				E: []string{"plugh", "xyzzy"},
				G: true,
				H: true,
				I: true,
			},
			check: func(t *testing.T, gotI, wantI interface{}) {
				got := gotI.(*astruct)
				if got.I {
					t.Error("got field I true, want false")
				} else {
					got.I = true
					if !reflect.DeepEqual(got, wantI) {
						t.Errorf("got %+v, want %+v", got, wantI)
					}
				}
			},
		},
	}

	ctx := context.Background()

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			storage := new(memory.Storage)
			ref, err := Marshal(ctx, storage, c.obj)
			if err != nil {
				t.Fatal(err)
			}

			objTyp := reflect.TypeOf(c.obj)
			dupVal := reflect.New(objTyp)

			err = Unmarshal(ctx, storage, ref, dupVal.Interface())
			if err != nil {
				t.Fatal(err)
			}

			if c.check != nil {
				c.check(t, dupVal.Elem().Interface(), c.obj)
			} else if !reflect.DeepEqual(dupVal.Elem().Interface(), c.obj) {
				t.Errorf("got %v, want %v", dupVal.Elem().Interface(), c.obj)
			}

			ch := make(chan blob.SizedRef)
			go storage.EnumerateBlobs(ctx, ch, "", -1)
			for sref := range ch {
				func() {
					r, _, err := storage.Fetch(ctx, sref.Ref)
					if err != nil {
						t.Fatal(err)
					}
					defer r.Close()
					b, err := ioutil.ReadAll(r)
					if err != nil {
						t.Fatal(err)
					}
					log.Printf("* %s: %s", sref.Ref, string(b))
				}()
			}
		})
	}
}

type astruct struct {
	A int
	B int    `pk:"b"`
	C string `pk:"a-string"`
	D []string
	E []string `pk:",external"`
	F *astruct `pk:",omitempty"`
	G bool
	H bool `pk:"inline"`
	I bool `pk:"-"`
}
