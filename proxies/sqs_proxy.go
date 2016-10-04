package proxies

import (
    "fmt"
    "os"

    "github.com/aws/aws-sdk-go/aws"
    "github.com/aws/aws-sdk-go/aws/credentials"
    "github.com/aws/aws-sdk-go/aws/session"
    "github.com/aws/aws-sdk-go/service/sqs"

    "thrift_flash/config"
)

type SqsProxy struct{
    AwsConfig      config.AwsConfig
    QueueName      string
    Conn           *sqs.SQS
}

func CreateSqsProxy(queuename string, credspath string) SqsProxy{
    aws_config := config.Create_config(credspath)
    aws_config.Read_aws_keys()

    s := SqsProxy{
        AwsConfig:  aws_config,
        QueueName:  queuename,
        Conn:       &sqs.SQS{},
    }

    s.Connect()
    return s
}

func (s *SqsProxy) Connect(){
    session := session.New(&aws.Config{
        Region:      aws.String("us-west-1"),
        Credentials: credentials.NewStaticCredentials(s.AwsConfig.Keys.Access_key_id, s.AwsConfig.Keys.Secret_access_key, ""),
    })

    svc := sqs.New(session)

    s.Conn = svc
}

func (s *SqsProxy) ListQueues(prefix string) {
    params := &sqs.ListQueuesInput{
        QueueNamePrefix: aws.String(prefix),
    }

    resp, err := s.Conn.ListQueues(params)
    if err != nil {
        fmt.Println(err.Error())
        return
    }
    fmt.Println("Here is the output from response")
    fmt.Println(resp)
}

// Takes in a json message which we marshall and convert to string
func (s *SqsProxy) WriteMessage(queuename string) {

    queue_url := s.GetQueueUrl("kew_devc_sagarp_sagarp_experiment", "528741615426")
    params := &sqs.SendMessageInput{
        MessageBody:  aws.String("Trial message from flash"), // Required
        QueueUrl:     queue_url.QueueUrl, // Required
        DelaySeconds: aws.Int64(1),
    }
    resp, err := s.Conn.SendMessage(params)

    if err != nil {
        // Print the error, cast err to awserr.Error to get the Code and
        // Message from an error.
        fmt.Println(err.Error())
        return
    }

    // Pretty-print the response data.
    fmt.Println("Here is the response from AWS ", resp)
}

func (s *SqsProxy) WriteMessageBatch() {
    params := &sqs.SendMessageBatchInput{
    Entries: []*sqs.SendMessageBatchRequestEntry{ // Required
        { // Required
            Id:           aws.String("String"), // Required
            MessageBody:  aws.String("String"), // Required
            DelaySeconds: aws.Int64(1),
            MessageAttributes: map[string]*sqs.MessageAttributeValue{
                "Key": { // Required
                    DataType: aws.String("String"), // Required
                     BinaryListValues: [][]byte{
                         []byte("PAYLOAD"), // Required
                         // More values...
                     },
                     BinaryValue: []byte("PAYLOAD"),
                     StringListValues: []*string{
                         aws.String("String"), // Required
                         // More values...
                     },
                     StringValue: aws.String("String"),
                },
                    // More values...
            },
        },
            // More values...
        },
        QueueUrl: aws.String("String"), // Required
    }

    resp, err := s.Conn.SendMessageBatch(params)

    if err != nil {
        // Print the error, cast err to awserr.Error to get the Code and
        // Message from an error.
        fmt.Println(err.Error())
        return
    }

    fmt.Println("This is the response we got from AWS", resp)
}

func (s *SqsProxy) GetQueueUrl(queue_name string, account_no string) *sqs.GetQueueUrlOutput {
    params := &sqs.GetQueueUrlInput{
        QueueName:              aws.String(queue_name), // Required
        QueueOwnerAWSAccountId: aws.String(account_no),
    }

    resp, err := s.Conn.GetQueueUrl(params)
    if err != nil {
        // Print the error, cast err to awserr.Error to get the Code and
        // Message from an error.
        fmt.Println(err.Error())
        os.Exit(1)
    }
    fmt.Println("The queue url is")
    fmt.Println(resp)
    return resp
}
