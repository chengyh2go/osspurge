package main

import (
	"flag"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"log"
	"os"
	"osspurge/utils"
	"sort"
	"strings"
	"time"
)

var (
	Help bool
	OssEndpoint string
	OssAccessKeyID string
	OssAccessKeySecret string
	OssBucketName string
	OssExpireDay int
)

func init()  {
	flag.BoolVar(&Help, "help", false, "oss对象改名")
	flag.StringVar(&OssEndpoint, "ossEndpoint",
		"","输入OSS的Endpoint")
	flag.StringVar(&OssAccessKeyID, "accessKeyID", "","输入accessKeyID")
	flag.StringVar(&OssAccessKeySecret, "accessKeySecret", "","输入accessKeySecret")
	flag.StringVar(&OssBucketName, "bucketName", "","输入bucket name")
	flag.IntVar(&OssExpireDay, "OssExpireDay", 3,"设置过期天数，默认为3")
	flag.Usage = usage
}

func usage() {
	_, _ = fmt.Fprintf(os.Stdout, `oss对象改名: v1.0
Options:
`)
	flag.PrintDefaults()
}

type dateList []string

func (list dateList) Len() int {
	return len(list)
}
func (list dateList) Less(i, j int) bool {
	return list[i] > list[j]
}
func (list dateList) Swap(i, j int) {
	list[i], list[j] = list[j], list[i]
}


func main() {

	//解析命令行参数
	flag.Parse()

	if Help {
		flag.Usage()
	} else {
		// 创建OSSClient实例。
		client, err := oss.New(OssEndpoint, OssAccessKeyID, OssAccessKeySecret)
		if err != nil {
			fmt.Println("Error:", err)
			os.Exit(-1)
		}
		// 获取存储空间。
		bucket, err := client.Bucket(OssBucketName)
		if err != nil {
			fmt.Println("Error:", err)
			os.Exit(-1)
		}

		//根据当前时间获取超时时间
		log.Println("开始查找过期对象并清除...")
		startTime := time.Now()

		//要删除过期文件夹，如果文件夹非空，则需要将文件夹下的所有object删除后才能删除该文件夹。
		//所以需要找出包含过期时间文件夹下的所有object，并删除
		//具体操作：先过滤掉空目录，如DM_DT/20211013/，找出DM/20211013/T_ODS_LC_T_USER_INFO_HV.flg这样的过期文件

		listObjects, _ := bucket.ListObjects()

		/*首先构造2个数据库，格式：map[string]string
		1，current库
		2，expired库
		 */

		//首先构造出current库
		currentDB := make(map[string][]string)
		for _, v := range listObjects.Objects {
			//找出：DM/20211013/T_ODS_LC_T_USER_INFO_HV.flg这样的key
			splitStr := strings.Split(v.Key, "/")
			destPrefix := splitStr[0]
			if (strings.HasPrefix(splitStr[1],"2") || strings.HasPrefix(splitStr[1],"1")) &&
				len(splitStr[1]) == 8 { //只针对DM/20211013这种格式的object做处理
				//用date变量接收splitStr中的日期字符串
				date := splitStr[1]

				//如果日期currentDb中的日期字符串数组还没有包括date，才append进去
				if !utils.IsElementExists(currentDB[destPrefix],date) {
					currentDB[destPrefix] = append(currentDB[destPrefix],date)
				}
			}
		}

		log.Println("找到的日期信息：",currentDB)

		//开始build expiredDB
		expiredDB := make(map[string][]string)

		for k,v := range currentDB {
			if len(v) > 3 {
				needSortArr := dateList(v)
				sort.Sort(needSortArr) //倒序排列
				expiredDB[k] = needSortArr[OssExpireDay:]  //从第4个元素开始取
			}
		}
		log.Println("找到的过期信息：",expiredDB)

		//清除过期的对象文件
		for _, v := range listObjects.Objects {
			if !strings.HasSuffix(v.Key,"/") {
				splitStr := strings.Split(v.Key, "/")
				destPrefix := splitStr[0]
				if (strings.HasPrefix(splitStr[1],"2") || strings.HasPrefix(splitStr[1],"1")) &&
					len(splitStr[1]) == 8 {
					date := splitStr[1]
					if utils.IsElementExists(expiredDB[destPrefix],date) {
						//fmt.Println("找到删除的对象：",v.Key)
						err := bucket.DeleteObject(v.Key)
						if err != nil {
							log.Printf("删除对象：%v 失败!\n",v.Key)
						} else {
							log.Printf("删除过期对象：%v 成功！\n",v.Key)
						}
					}
				}
			}
		}

		//过期对象文件清除之后，清除过期的空目录
		for _, v := range listObjects.Objects {
			if strings.HasSuffix(v.Key,"/") {
				splitStr := strings.Split(v.Key, "/")
				destPrefix := splitStr[0]
				if (strings.HasPrefix(splitStr[1],"2") || strings.HasPrefix(splitStr[1],"1")) &&
					len(splitStr[1]) == 8 {
					date := splitStr[1]
					if utils.IsElementExists(expiredDB[destPrefix],date) {
						//fmt.Println("找到删除的目录：",v.Key)
						err := bucket.DeleteObject(v.Key)
						if err != nil {
							log.Printf("删除对象：%v 失败!\n",v.Key)
						} else {
							log.Printf("删除过期对象：%v 成功！\n",v.Key)
						}
					}
				}
			}
		}

		log.Printf("oss过期对象清除任务完成，耗时：%v 毫秒",int(time.Now().Sub(startTime).Milliseconds()))
	}
}
