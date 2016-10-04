package config

import(
    "fmt"
    "io/ioutil"
    "os"

    "gopkg.in/yaml.v2"
)

type AwsConfig struct {
    Keys        AwsKeys
    Filename    string
}

type AwsKeys struct {
    Access_key_id string
    Secret_access_key string
}

func Create_config(filename string) AwsConfig{
    awskeys := AwsKeys{}
    config := AwsConfig{
        Keys:        awskeys,
        Filename:    filename,
    }

    return config
}

func (c *AwsConfig)Read_aws_keys() AwsKeys{
    data := c.Readfile()
    yaml_load := make(map[string]interface{})

    err := yaml.Unmarshal([]byte(data), &yaml_load)
    if err!= nil{
        fmt.Println("Failed to read keys from %s", c.Filename)
        os.Exit(1)
    }

    c.Keys = AwsKeys{
        Access_key_id:      yaml_load["aws_access_key_id"].(string),
        Secret_access_key:  yaml_load["aws_secret_access_key"].(string),
    }

    fmt.Println("Here is the value of AWS KEYS:", c)

    return c.Keys
}

func (c AwsConfig) Readfile() []byte{
    data, err := ioutil.ReadFile(c.Filename)
    if err != nil {
        fmt.Println("Could not open file", c.Filename, err)
        os.Exit(1)
    }
    return data
}
