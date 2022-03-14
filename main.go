package main

import (
	"context"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"time"
)

type Community struct {
	ID int
	Province string
	City string
	District string
	Name string
	Url string
	DetailAddress string
	Coord string  `gorm:"column:coord"`
	Price string
	PropertyType string
	PropertyFee string
	Area string
	HouseCount string
	CompletionTime string
	ParkingCount string
	PlotRatio string
	GreeningRate string
	PropertyCompany string
	Developers string
	DistractedMachong string
	ReturnWejd string `gorm:"column:returnwejd"`
	ReturnTejd string `gorm:"column:returntejd"`
	ReportTime string
	GlobalDist string

}

var DbConn *gorm.DB


//MySQL数据库连接信息
const DSN = "root:houboxue@tcp(106.52.8.85:3306)/jobspider?charset=utf8&parseTime=True&loc=Local"
//指定驱动
const DRIVER = "mysql"

var db *gorm.DB
var pool *redis.Pool   //创建redis连接池


func init() {
	var err error
	db,err = gorm.Open(DRIVER,DSN)
	if err != nil{
		panic(err)
	}
	setdb := redis.DialDatabase(2)    // 设置4号数据库
	setPasswd := redis.DialPassword("houboxue") // 设置redis连接密码
	pool = &redis.Pool{     //实例化一个连接池
		MaxIdle:16,    		//最初的连接数量
		MaxActive:0,    	//连接池最大连接数量,不确定可以用0（0表示自动定义），按需分配
		IdleTimeout:300,    //连接关闭时间 300秒 （300秒不使用自动关闭）
		Dial: func() (redis.Conn ,error){     //要连接的redis数据库
			return redis.Dial("tcp","47.112.123.168:6379",setdb,setPasswd)
		},
	}
}
func ConnectToDB(uri, name string, timeout time.Duration, num uint64) (*mongo.Database, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	o := options.Client().ApplyURI(uri)
	o.SetMaxPoolSize(num)
	client, err := mongo.Connect(ctx, o)
	if err != nil {
		return nil, err
	}
	return client.Database(name), nil
}

func main() {
	db.SingularTable(true)
	db.AutoMigrate(&Community{})

	var commi []Community
	var commi2 []Community
	find :=db.Debug().Table("community_copy1").Select("coord").Find(&commi)

	if find.Error != nil{
		panic(find.Error)
	}
	//coord1 := commi[0].Coord
	//coord2 := commi[1].Coord
	//fmt.Println("查询表中name字段参数为age的记录：", coord1)
	//fmt.Println("查询表中name字段参数为age的记录：", coord2)
	//fmt.Println(reflect.TypeOf(findcoord))
	//找出总记录数
	allnumber := count()
	//println(allnumber)
	// 连接到MongoDB
	client, err := ConnectToDB("mongodb://47.112.123.168:27017", "xiaoqu_spider", 1000*time.Millisecond, 10)

	collection := client.Collection("afterfilterxiaoqu")

	if err != nil {
		log.Fatal(err)
	}
	// 检查连接
	err = client.Client().Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Connected to MongoDB!")


	for i := 0; i > allnumber; i++ {
		findcoord := commi[i].Coord

		// println(findcoord)
		c := pool.Get()  //从连接池，取一个链接
		defer c.Close()  //运行结束 ，把连接放回池子里面

		isKeyExit, err := redis.Bool(c.Do("EXISTS", findcoord))

		if err != nil {
			panic(err)

		} else {
			if isKeyExit {
				db.Table("community_copy1").Where("coord = ?", findcoord).First(&commi2)
				//println(commi2[0].Name)
				//println(commi2[0].Coord)
				//println(commi2[0].Price)
				s1 := Community{commi2[0].ID, commi2[0].Province,commi2[0].City,commi2[0].District,
					commi2[0].Name,commi2[0].Url,commi2[0].DetailAddress,
					commi2[0].Coord,commi2[0].Price,commi2[0].PropertyType,commi2[0].PropertyFee,commi2[0].Area,commi2[0].HouseCount,commi2[0].CompletionTime,
					commi2[0].ParkingCount,commi2[0].PlotRatio,commi2[0].GreeningRate,commi2[0].PropertyCompany,commi2[0].Developers,
					commi2[0].DistractedMachong,commi2[0].ReturnWejd,commi2[0].ReturnTejd,commi2[0].ReportTime,commi2[0].GlobalDist}
				collection.InsertOne(context.TODO(),s1)
				println("insert Mongo....")
				// println(first)
			} else {
				continue
			}
		}
	}
	pool.Close() //关闭连接池
	fmt.Println("已完成")
}
//查询数据库中的项目条数
func count()  int{
	var count int
	db.Model(&Community{}).Table("community_copy1").Count(&count)
	return count
}
