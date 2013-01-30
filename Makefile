NAME = wiki2es
VERSION = $(shell git ver)
BIN = $(NAME)-$(VERSION)
S3HOME = s3://download.elasticsearch.org/wiki2es

clean:
	lein clean

package: clean
	echo -n $(VERSION) >etc/version.txt
	lein bin

release: package
	s3cmd -c $(S3CREDS) put -P target/$(BIN) $(S3HOME)/$(BIN)
	s3cmd -c $(S3CREDS) cp $(S3HOME)/$(BIN) $(S3HOME)/$(NAME)
