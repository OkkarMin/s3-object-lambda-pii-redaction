### S3 Object Lambda

```bash
aws s3api get-object --bucket arn:aws:s3-object-lambda:ap-southeast-1:850754771995:accesspoint/s3-olap-demo-olap --key output.json transformed_output.json
```

### S3 Original Get

```bash
aws s3api get-object --bucket s3-olap-demo-bucket --key output.json transformed_output.json
```
