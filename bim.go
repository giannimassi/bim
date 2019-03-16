package main

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"os"
	"path/filepath"

	"github.com/minio/minio-go"
)

type Config struct {
	URL    string
	Region string
	Key    string
	Secret string
	Bucket string
	Dir    string
	File   string
}

func main() {

	// Config
	var cfg Config
	viper.SetConfigName("config")
	viper.AddConfigPath(".")
	err := viper.ReadInConfig()
	if err != nil {
		fmt.Println("No config file found")
	}

	pflag.String("url", "", "repository url")
	pflag.String("region", "", "repository region")
	pflag.String("key", "", "repository key")
	pflag.String("secret", "", "repository secret")
	pflag.String("bucket", "", "bucket name")
	pflag.String("dir", "", "dir in repository")
	pflag.String("file", "", "file to upload")

	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)
	err = viper.Unmarshal(&cfg)
	exitIfErrWithConfig(err, cfg)

	fmt.Printf("Uploading %s\n", cfg.File)

	err = uploadFile(cfg)
	exitIfErrWithConfig(err, cfg)
	fmt.Printf("Done\n")
}

func exitIfErrWithConfig(err error, cfg Config) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Err: %v", err)
		os.Exit(1)
	}
}

func uploadFile(cfg Config) error {
	client, err := newClient(cfg)
	if err != nil {
		return errors.Annotatef(err, "while creating storage client")
	}

	// Check to see if the bucket exists
	exists, err := client.BucketExists(cfg.Bucket)
	if err != nil {
		err = errors.Annotatef(err, "while checking if bucket %s exists", cfg.Bucket)
		return err
	}

	// If the bucket doesn't exist, create it
	if !exists {
		err = client.MakeBucket(cfg.Bucket, "")
		if err != nil {
			err = errors.Annotatef(err, "while creating bucket %s", cfg.Bucket)
			return err
		}
	}

	// Upload the zip file with FPutObject
	_, err = client.FPutObject(cfg.Bucket, filepath.Join(cfg.Dir, filepath.Base(cfg.File)), cfg.File, minio.PutObjectOptions{})
	if err != nil {
		err = errors.Annotatef(err, "while uploading %s to bucket %s", cfg.File, cfg.Bucket)
		return err
	}

	return nil
}

func newClient(cfg Config) (*minio.Client, error) {
	c, err := minio.New(fmt.Sprintf("%s.%s", cfg.Region, cfg.URL), cfg.Key, cfg.Secret, true)
	if err != nil {
		return nil, errors.Annotatef(err, "while creating new client (%s)", fmt.Sprintf("%s.%s", cfg.Region, cfg.URL))
	}

	return c, nil
}
