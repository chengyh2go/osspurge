package utils

import "time"

func IsElementExists(arr []string, element string) bool {
	var res bool
	for _,v := range arr {
		if v == element {
			res = true
		}
	}
	return res
}

func GetExpireDay(expireValue int) string {
	inputDayTime, _ := time.Parse("20060102", time.Now().Format("20060102"))
	previousDay := inputDayTime.AddDate(0,0,expireValue - expireValue*2)
	return previousDay.Format("20060102")
}
