package speedtest

import "testing"

func TestFetchUserInfo(t *testing.T) {
	client, _ := discoveryFixture(t)
	user, err := client.FetchUserInfo()
	if err != nil {
		t.Fatal(err)
	}
	if user.IP != "192.0.2.10" || user.Province != "测试省" || user.City != "测试市" || user.Operator != "测试运营商" || user.Isp != user.Operator {
		t.Fatalf("unexpected user: %+v", user)
	}
	if user.Lat != "" || user.Lon != "" {
		t.Fatalf("unexpected coordinates: %+v", user)
	}
	cached, err := client.FetchUserInfo()
	if err != nil || cached != user || client.User != user {
		t.Fatalf("cached user: %v, %v", cached, err)
	}
}
