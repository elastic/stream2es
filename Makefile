LEIN ?= lein
NAME = stream2es
VERSION = $(shell git ver)
BIN = $(NAME)-$(VERSION)
S3HOME = s3://download.elasticsearch.org/$(NAME)

clean:
	$(LEIN) clean

package: clean
	mkdir -p etc
	echo $(VERSION) >etc/version.txt
	LEIN_SNAPSHOTS_IN_RELEASE=yes $(LEIN) bin

install: package
	cp target/$(BIN) ~/bin/$(NAME)

release: package
	s3cmd -c $(S3CREDS) put -P target/$(BIN) $(S3HOME)/$(BIN)
	s3cmd -c $(S3CREDS) cp $(S3HOME)/$(BIN) $(S3HOME)/$(NAME)
