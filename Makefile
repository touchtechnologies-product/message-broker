.PHONY: test

test:
	go test --cover

publish:
	go mod tidy
	git add .
	git commit -m "Version $(v): $(msg)"
	git tag $(v)
	git push origin $(v)
	