package main

import (
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	_ "github.com/spf13/viper"
)

var (
	isLambda    bool
	sqsSvc      *sqs.SQS
	dummyText   string
	sqsEndpoint string
	loop        int64
)

func init() {
	log.Info("Initialize SQS Publisher")

	isLambda = len(os.Getenv("_LAMBDA_SERVER_PORT")) > 0
	if isLambda {
		log.SetLevel(log.InfoLevel)
		viper.SetEnvPrefix("SNSPUBLISH")
		viper.BindEnv("REGION", "TEXT", "ENDPOINT", "LOOP")
	} else {
		log.SetReportCaller(true)
		log.SetLevel(log.DebugLevel)
		viper.SetConfigName("config")
		viper.SetConfigType("yaml")
		viper.AddConfigPath(".")
		err := viper.ReadInConfig()
		if err != nil {
			log.Fatalln(err)
		}
	}

	region := viper.GetString("REGION")
	dummyText = viper.GetString("TEXT")
	sqsEndpoint = viper.GetString("ENDPOINT")
	loop = viper.GetInt64("LOOP")

	sess, err := session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region: aws.String(region),
		},
		SharedConfigState: session.SharedConfigEnable,
	})
	if err != nil {
		log.Fatalln(err)
	}
	sqsSvc = sqs.New(sess)

}

func main() {
	for loop > 0 {
		PublishMsg()
		loop--
	}
}

func PublishMsg() {
	log.Debug("Start PublishMsg")
	unixTime := time.Now().UnixNano()
	_, err := sqsSvc.SendMessage(&sqs.SendMessageInput{
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"CreateTimestamp": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(strconv.FormatInt(unixTime, 10)),
			},
		},
		MessageBody: aws.String(dummyText),
		QueueUrl:    aws.String(sqsEndpoint),
	})

	if err != nil {
		log.Warn(err.Error())
	}
}
