package main

import (
	"flag"
	"fmt"
	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"log"
	"os"
	"osspurge/utils"
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
		"oss-cn-beijing.aliyuncs.com","输入OSS的Endpoint")
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

func purgeExpiredOssObject(bucket *oss.Bucket,objectFullName,expireDay string) {
	splits := strings.Split(objectFullName,"/")
	//fmt.Println(v.Key,splits,len(splits))
	if strings.Contains(splits[1],"20") && splits[1] <= expireDay {
		//fmt.Println(objectFullName)
		err := bucket.DeleteObject(objectFullName)
		if err != nil {
			log.Printf("删除对象：%v 失败!\n",objectFullName)
		} else {
			log.Printf("删除过期对象：%v 成功！\n",objectFullName)
		}
	}
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
		expireDay := utils.GetExpireDay(OssExpireDay)
		log.Printf("根据输入的过期天数：%v，得到的过期时间是：%v\n",OssExpireDay,expireDay)
		log.Println("开始查找过期对象并清除...")
		startTime := time.Now()

		lsRes, _ := bucket.ListObjects()
		for _, v := range lsRes.Objects {
			//要删除过期文件夹，如果文件夹非空，则需要将文件夹下的所有object删除后才能删除该文件夹。
			//所以需要找出包含过期时间文件夹下的所有object，并删除
			//具体操作：先过滤掉空目录，如DM_DT/20211013/，找出DM/20211013/T_ODS_LC_T_USER_INFO_HV.flg这样的过期文件
			if !strings.HasSuffix(v.Key, "/") {
				purgeExpiredOssObject(bucket, v.Key, expireDay)
			}
		}

		//过期对象文件清除之后，清除过期的空目录
		lsRes, _ = bucket.ListObjects()
		for _, v := range lsRes.Objects {
			if strings.HasSuffix(v.Key, "/") {
				purgeExpiredOssObject(bucket, v.Key, expireDay)
			}
		}
		log.Printf("oss过期对象清除任务完成，耗时：%v 毫秒",int(time.Now().Sub(startTime).Milliseconds()))
	}
}
