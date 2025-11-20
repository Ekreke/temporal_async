package convert

import (
	"context"
	"github.com/bytedance/sonic"
	"net/url"
	"strings"
	"testing"
)

func TestLoadPbDescriptor(t *testing.T) {
	r, err := loadPbDescriptor(context.Background(), "https://github.acme.red/mapper/idl/blob/main/proto/mapper/taskargs/v1/args.proto", "")
	if err != nil {
		t.Fatal(err)
	}
	fr, err := findMessageDescriptor(r, "ServiceProbeTaskData")
	if err != nil {
		t.Fatal(err)
	}

	println(r.GetPackage(), fr.GetFullyQualifiedName())
}

//func TestConvJsonToPB(t *testing.T) {
//	var a = map[string]interface{}{
//		"ip":     "123",
//		"domain": "1111",
//		"rate":   233,
//	}
//	taskData, _ := sonic.Marshal(a)
//	aa, err := JSONToPB(context.Background(), "https://github.acme.red/mapper/idl/blob/main/proto/mapper/taskargs/v1/args.proto", "ServiceProbeTaskData", taskData)
//	if err != nil {
//		t.Fatal(err)
//	}
//	println(aa.String())
//}

func TestConvertToRawURL(t *testing.T) {
	u, err := url.Parse("https://github.acme.red/mapper/idl/blob/main/proto/mapper/taskargs/v1/args.proto")
	if err != nil {
		panic(err)
	}
	println(u.Scheme, u.Host, u.RequestURI())
	uPath := strings.Split(u.Path, "/")
	var pb = githubContent{}
	var gPath = make([]string, 0)
	for _, pth := range uPath {
		if pth == "" || pth == "blob" {
			continue
		}
		if pb.Owner == "" {
			pb.Owner = pth
			continue
		}
		if pb.Repo == "" {
			pb.Repo = pth
			continue
		}
		if pb.Branch == "" {
			pb.Branch = pth
			continue
		}
		gPath = append(gPath, pth)
	}
	pb.Path = strings.Join(gPath, "/")
	println(sonic.MarshalString(pb))
}
