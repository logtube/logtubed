package common

import (
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

var (
	errDefaultsInvalidType = errors.New("SetDefaults(): not a struct pointer")
)

const (
	defaultsTagName = "default"
)

// DefaultsSetter is an interface for setting default values
type DefaultsSetter interface {
	SetDefaults()
}

func invokeDefaultsSetter(v interface{}) {
	if ds, ok := v.(DefaultsSetter); ok {
		ds.SetDefaults()
	}
}

func isInitialValue(field reflect.Value) bool {
	return reflect.DeepEqual(reflect.Zero(field.Type()).Interface(), field.Interface())
}

// IsInitialValue returns true when the given value is an initial value of its type
func IsInitialValue(v interface{}) bool {
	return isInitialValue(reflect.ValueOf(v))
}

func resolveEnvDefaults(s string) string {
	if !strings.HasPrefix(s, "$") {
		return s
	}
	if i := strings.Index(s, "|"); i != -1 {
		if v := os.Getenv(s[1:i]); len(v) > 0 {
			return v
		} else {
			s = s[i+1:]
		}
	}
	return s
}

// SetDefaults initializes members in a struct referenced by a pointer.
// Maps and slices are initialized by `make` and other primitive types are set with default values.
// `ptr` should be a struct pointer
func SetDefaults(ptr interface{}) error {
	if reflect.TypeOf(ptr).Kind() != reflect.Ptr {
		return errDefaultsInvalidType
	}

	v := reflect.ValueOf(ptr).Elem()
	t := v.Type()

	if t.Kind() != reflect.Struct {
		return errDefaultsInvalidType
	}

	for i := 0; i < t.NumField(); i++ {
		if defaultVal := t.Field(i).Tag.Get(defaultsTagName); defaultVal != "-" {
			if err := setFieldDefaults(v.Field(i), resolveEnvDefaults(defaultVal)); err != nil {
				return err
			}
		}
	}

	return nil
}

func shouldSetFieldDefaults(field reflect.Value, tag string) bool {
	switch field.Kind() {
	case reflect.Struct:
		return true
	case reflect.Slice:
		return field.Len() > 0 || tag != ""
	}
	return tag != ""
}

func setFieldDefaults(field reflect.Value, defaultVal string) error {
	if !field.CanSet() {
		return nil
	}

	if !shouldSetFieldDefaults(field, defaultVal) {
		return nil
	}

	if isInitialValue(field) {
		switch field.Kind() {
		case reflect.Bool:
			if val, err := strconv.ParseBool(defaultVal); err == nil {
				field.Set(reflect.ValueOf(val).Convert(field.Type()))
			}
		case reflect.Int:
			if val, err := strconv.ParseInt(defaultVal, 10, 64); err == nil {
				field.Set(reflect.ValueOf(int(val)).Convert(field.Type()))
			}
		case reflect.Int8:
			if val, err := strconv.ParseInt(defaultVal, 10, 8); err == nil {
				field.Set(reflect.ValueOf(int8(val)).Convert(field.Type()))
			}
		case reflect.Int16:
			if val, err := strconv.ParseInt(defaultVal, 10, 16); err == nil {
				field.Set(reflect.ValueOf(int16(val)).Convert(field.Type()))
			}
		case reflect.Int32:
			if val, err := strconv.ParseInt(defaultVal, 10, 32); err == nil {
				field.Set(reflect.ValueOf(int32(val)).Convert(field.Type()))
			}
		case reflect.Int64:
			if val, err := time.ParseDuration(defaultVal); err == nil {
				field.Set(reflect.ValueOf(val).Convert(field.Type()))
			} else if val, err := strconv.ParseInt(defaultVal, 10, 64); err == nil {
				field.Set(reflect.ValueOf(val).Convert(field.Type()))
			}
		case reflect.Uint:
			if val, err := strconv.ParseUint(defaultVal, 10, 64); err == nil {
				field.Set(reflect.ValueOf(uint(val)).Convert(field.Type()))
			}
		case reflect.Uint8:
			if val, err := strconv.ParseUint(defaultVal, 10, 8); err == nil {
				field.Set(reflect.ValueOf(uint8(val)).Convert(field.Type()))
			}
		case reflect.Uint16:
			if val, err := strconv.ParseUint(defaultVal, 10, 16); err == nil {
				field.Set(reflect.ValueOf(uint16(val)).Convert(field.Type()))
			}
		case reflect.Uint32:
			if val, err := strconv.ParseUint(defaultVal, 10, 32); err == nil {
				field.Set(reflect.ValueOf(uint32(val)).Convert(field.Type()))
			}
		case reflect.Uint64:
			if val, err := strconv.ParseUint(defaultVal, 10, 64); err == nil {
				field.Set(reflect.ValueOf(val).Convert(field.Type()))
			}
		case reflect.Uintptr:
			if val, err := strconv.ParseUint(defaultVal, 10, 64); err == nil {
				field.Set(reflect.ValueOf(uintptr(val)).Convert(field.Type()))
			}
		case reflect.Float32:
			if val, err := strconv.ParseFloat(defaultVal, 32); err == nil {
				field.Set(reflect.ValueOf(float32(val)).Convert(field.Type()))
			}
		case reflect.Float64:
			if val, err := strconv.ParseFloat(defaultVal, 64); err == nil {
				field.Set(reflect.ValueOf(val).Convert(field.Type()))
			}
		case reflect.String:
			field.Set(reflect.ValueOf(defaultVal).Convert(field.Type()))

		case reflect.Slice:
			ref := reflect.New(field.Type())
			ref.Elem().Set(reflect.MakeSlice(field.Type(), 0, 0))
			if defaultVal != "" && defaultVal != "[]" {
				if err := json.Unmarshal([]byte(defaultVal), ref.Interface()); err != nil {
					return err
				}
			}
			field.Set(ref.Elem().Convert(field.Type()))
		case reflect.Map:
			ref := reflect.New(field.Type())
			ref.Elem().Set(reflect.MakeMap(field.Type()))
			if defaultVal != "" && defaultVal != "{}" {
				if err := json.Unmarshal([]byte(defaultVal), ref.Interface()); err != nil {
					return err
				}
			}
			field.Set(ref.Elem().Convert(field.Type()))
		case reflect.Struct:
			ref := reflect.New(field.Type())
			if defaultVal != "" && defaultVal != "{}" {
				if err := json.Unmarshal([]byte(defaultVal), ref.Interface()); err != nil {
					return err
				}
			}
			field.Set(ref.Elem())
		case reflect.Ptr:
			field.Set(reflect.New(field.Type().Elem()))
		}
	}

	switch field.Kind() {
	case reflect.Ptr:
		if err := setFieldDefaults(field.Elem(), defaultVal); err != nil {
			return err
		}
		invokeDefaultsSetter(field.Interface())
	case reflect.Struct:
		ref := reflect.New(field.Type())
		ref.Elem().Set(field)
		if err := SetDefaults(ref.Interface()); err != nil {
			return err
		}
		invokeDefaultsSetter(ref.Interface())
		field.Set(ref.Elem())
	case reflect.Slice:
		for j := 0; j < field.Len(); j++ {
			if err := setFieldDefaults(field.Index(j), defaultVal); err != nil {
				return err
			}
		}
	}

	return nil
}
