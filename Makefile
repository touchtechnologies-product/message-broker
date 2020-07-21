.PHONY: publish

publish:
	go mod tidy
	git add .
	git commit -m "Version $(v): $(msg)"
	git tag $(v)
	git push origin $(v)
	git push
	