package utils

import (
	"fmt"
	"testing"
)

func TestGetExpireDayByInputDay(t *testing.T) {
	expireDay := GetExpireDay(3)
	fmt.Println(expireDay)
}
