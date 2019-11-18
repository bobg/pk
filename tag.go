package pk

import (
	"reflect"
	"strings"
)

type options struct {
	inline    bool
	external  bool
	omitEmpty bool
	omit      bool
}

// tag syntax, inspired by encoding/json:
//  `pk:"-"` means omit the field
//  `pk:"name"` means use this name
//  `pk:"name,option,option"` means use this name and set these options
//  `pk:",option,option"` means just set these options
//
// available options are:
//  inline: encode the field as an inline value, not a ref to a separate blob
//  external: store blob for containers (slices, arrays, and maps)
//    (by default, the container is inlined and the elements are blobrefs)
//  omitEmpty: skip the field if it has a zero value
func parseTag(f reflect.StructField) (string, options) {
	var (
		name = f.Name
		o    options
	)
	if t, ok := f.Tag.Lookup("pk"); ok {
		switch t {
		case "": // ok
		case "-":
			o.omit = true
		default:
			items := strings.Split(t, ",")
			if items[0] != "" {
				name = items[0]
			}
			for _, item := range items[1:] {
				switch item {
				case "inline":
					o.inline = true
				case "external": // xxx need a better name than external
					o.external = true
				case "omitempty":
					o.omitEmpty = true
				}
			}
		}
	}
	return /* ...and bingo was his */ name, o
}
