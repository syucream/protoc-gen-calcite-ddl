package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"strings"

	"github.com/golang/protobuf/protoc-gen-go/descriptor"
	"google.golang.org/protobuf/proto"

	plugin "github.com/golang/protobuf/protoc-gen-go/plugin"
)

// https://calcite.apache.org/docs/reference.html#data-types
var primitives = map[descriptor.FieldDescriptorProto_Type]string{
	descriptor.FieldDescriptorProto_TYPE_DOUBLE:  "DOUBLE",
	descriptor.FieldDescriptorProto_TYPE_FLOAT:   "FLOAT",
	descriptor.FieldDescriptorProto_TYPE_INT64:   "BIGINT",
	descriptor.FieldDescriptorProto_TYPE_UINT64:  "BIGINT",
	descriptor.FieldDescriptorProto_TYPE_INT32:   "INT",
	descriptor.FieldDescriptorProto_TYPE_FIXED64: "BIGINT",
	descriptor.FieldDescriptorProto_TYPE_FIXED32: "INT",
	descriptor.FieldDescriptorProto_TYPE_BOOL:    "BOOLEAN",
	descriptor.FieldDescriptorProto_TYPE_STRING:  "STRING", // NOTE extended
	// descriptor.FieldDescriptorProto_TYPE_GROUP NOTE unsupported
	// descriptor.FieldDescriptorProto_TYPE_MESSAGE NOTE dig nested types
	descriptor.FieldDescriptorProto_TYPE_BYTES:    "BYTES", // NOTE extended
	descriptor.FieldDescriptorProto_TYPE_UINT32:   "INT",
	descriptor.FieldDescriptorProto_TYPE_ENUM:     "STRING",
	descriptor.FieldDescriptorProto_TYPE_SFIXED32: "INT",
	descriptor.FieldDescriptorProto_TYPE_SFIXED64: "BIGINT",
	descriptor.FieldDescriptorProto_TYPE_SINT32:   "INT",
	descriptor.FieldDescriptorProto_TYPE_SINT64:   "BIGINT",
}

func getTable(msg *descriptor.DescriptorProto) string {
	columns := []string{}
	for _, f := range msg.GetField() {
		// primitive types
		if t, ok := primitives[f.GetType()]; ok {
			columns = append(columns, fmt.Sprintf("  %s %s", f.GetName(), t))
		}

		// complex types

		// logical types
	}

	return fmt.Sprintf("CREATE TABLE %s (\n%s\n)", msg.GetName(), strings.Join(columns, ",\n"))
}

func main() {
	buf, err := ioutil.ReadAll(os.Stdin)
	if err != nil {
		log.Fatal(err)
	}

	var req plugin.CodeGeneratorRequest
	if err := proto.Unmarshal(buf, &req); err != nil {
		log.Fatal(err)
	}

	resp := plugin.CodeGeneratorResponse{}
	for _, f := range req.GetProtoFile() {
		for _, m := range f.GetMessageType() {
			resp.File = append(resp.File, &plugin.CodeGeneratorResponse_File{
				Name:    proto.String(m.GetName() + ".sql"),
				Content: proto.String(getTable(m)),
			})
		}
	}

	buf, err = proto.Marshal(&resp)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := os.Stdout.Write(buf); err != nil {
		log.Fatal(err)
	}
}
