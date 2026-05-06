package main

import(
	"github.com/gin-gonic/gin"
)

type User struct{
	Name string;
	Age int
}

func main(){
	router := gin.Default();
	router.GET("/ping", func(c *gin.Context){
		c.JSON(200,gin.H{
			"message":"pong",
		})
	})
	router.POST("/create-user", func(c *gin.Context){
		var user User;
		err := c.BindJSON(&user);
		if(err != nil){
			c.JSON(400,gin.H{
				"error":"Bad request",
			})
			return ;
		};
		
		c.JSON(200,gin.H{
			"message":"user created",
			"data":user,
		});
	})
	router.Run(":8001");

}