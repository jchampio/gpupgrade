package ci

// Generate dev-pipeline.yml and prod-pipeline.yml from template.yml using parse_template
//go:generate go run ./parser/parse_template.go ./template.yml generated/dev-pipeline.yml
//go:generate go run ./parser/parse_template.go -prod ./template.yml generated/prod-pipeline.yml
