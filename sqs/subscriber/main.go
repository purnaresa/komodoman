package main

import (
	"context"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	_ "github.com/spf13/viper"
)

var (
	isLambda bool
	sqsSvc   *sqs.SQS

	sqsEndpoint string
)

func init() {
	log.Info("Initialize SQS Subscriber")

	isLambda = len(os.Getenv("_LAMBDA_SERVER_PORT")) > 0
	if isLambda {
		log.SetLevel(log.InfoLevel)
		viper.SetEnvPrefix("SNSSUBSCRIBE")
		viper.BindEnv("REGION", "ENDPOINT")
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

	sqsEndpoint = viper.GetString("ENDPOINT")

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

func handler(ctx context.Context, sqsEvent events.SQSEvent) error {
	for _, message := range sqsEvent.Records {
		unixNow := time.Now().UnixNano()
		unixSentRaw := message.MessageAttributes["CreateTimestamp"]
		unixSent, errParse := strconv.ParseInt(*unixSentRaw.StringValue, 10, 64)
		if errParse != nil {
			log.Errorln(errParse)
			continue
		}
		proceesTime := (float64(unixNow) - float64(unixSent)) / 1000000000
		log.WithField("time", proceesTime).Warn("message received")

		_, errDelete := sqsSvc.DeleteMessage(&sqs.DeleteMessageInput{
			QueueUrl:      &sqsEndpoint,
			ReceiptHandle: &message.ReceiptHandle,
		})
		if errDelete != nil {
			log.Errorln(errDelete)
			continue
		}
	}

	return nil
}

func main() {
	if isLambda {
		lambda.Start(handler)
	} else {
		for true {
			msgResult, err := sqsSvc.ReceiveMessage(&sqs.ReceiveMessageInput{
				AttributeNames: []*string{
					aws.String(sqs.MessageSystemAttributeNameSentTimestamp),
				},
				MessageAttributeNames: []*string{
					aws.String(sqs.QueueAttributeNameAll),
				},
				QueueUrl:            &sqsEndpoint,
				MaxNumberOfMessages: aws.Int64(1),
			})
			if err != nil {
				log.Errorln(err)
				return
			}
			for _, message := range msgResult.Messages {
				unixNow := time.Now().UnixNano()
				unixSentRaw := message.MessageAttributes["CreateTimestamp"]
				unixSent, errParse := strconv.ParseInt(*unixSentRaw.StringValue, 10, 64)
				if errParse != nil {
					log.Errorln(errParse)
					continue
				}
				proceesTime := (float64(unixNow) - float64(unixSent)) / 1000000000
				log.WithField("time", proceesTime).Warn("message received")

				_, errDelete := sqsSvc.DeleteMessage(&sqs.DeleteMessageInput{
					QueueUrl:      &sqsEndpoint,
					ReceiptHandle: message.ReceiptHandle,
				})
				if errDelete != nil {
					log.Errorln(errDelete)
					continue
				}
			}

		}
	}
}
