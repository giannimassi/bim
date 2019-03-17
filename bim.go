package main

import (
	"fmt"
	"github.com/juju/errors"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"gopkg.in/yaml.v2"
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

	cfg := setupConfig()
	fmt.Printf("Uploading %s, with cfg %v\n", cfg.File, cfg)

	err := uploadFile(cfg)
	exitIfErr(err)
	fmt.Printf("Done\n")
}

func exitIfErr(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Err: %v\n", err)
		os.Exit(1)
	}
}

func setupConfig() Config {
	// Config
	var cfg Config
	ex, err := os.Executable()
	exitIfErr(err)

	viper.AddConfigPath(filepath.Dir(ex))
	viper.AddConfigPath(".")
	viper.SetConfigName("bim")
	err = viper.ReadInConfig()
	if err != nil {
		if _, ok := err.(viper.ConfigFileNotFoundError); ok {

			var defaultConfigPath = ex +".yaml"
			fmt.Println("Config file not found, creating default config as %s", defaultConfigPath)

			b, err := yaml.Marshal(cfg)
			exitIfErr(errors.Annotatef(err, "while marshalling config to file"))

			f, err := os.Create(defaultConfigPath)
			exitIfErr(errors.Annotatef(err, "while opening file %s to write config", defaultConfigPath))

			_, err = f.Write(b)
			exitIfErr(errors.Annotatef(err, "while writing config to file %s", defaultConfigPath))

		}
		exitIfErr(errors.Annotatef(err, "while reading config"))
	}

	pflag.StringP("url", "u", "", "repository url")
	pflag.StringP("region", "r","", "repository region")
	pflag.StringP("key", "k", "", "repository key")
	pflag.StringP("secret", "s","", "repository secret")
	pflag.StringP("bucket", "b", "", "bucket name")
	pflag.StringP("dir", "d", "", "dir in repository")
	pflag.StringP("file", "f", "", "file to upload")

	pflag.Parse()
	viper.BindPFlags(pflag.CommandLine)
	err = viper.Unmarshal(&cfg)
	exitIfErr(err)

	return cfg
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
