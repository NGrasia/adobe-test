#!/usr/bin/env bash

DIRNAME= "./bin"
S3_BUCKET = "placeholder"
TEMPLATE_PATH="${DIRNAME}/template.yaml"
BUNDLE_PATH="${DIRNAME}/output/packaged-template.yaml"
CODE_S3_BUCKET="codepipeline-us-east-1-58008411995"
CODE_S3_BUCKET_KEY='/shared/my_code'
DEPLOY_LOCATION="s3://$CODE_S3_BUCKET$CODE_S3_BUCKET_KEY"

function code_zip {
  echo "copying folder to env "

  cp -r my-sam-app/ my-sam-app/

  echo -e "Executing zip func"

  ZIP_FILE_NAME='hello_world_01.zip'
  zip -r $ZIP_FILE_NAME bin/ my-sam-app/ hello_world/
  echo -e "Uploading the zip file to S3"
  aws s3 cp $ZIP_FILE_NAME s3://$CODE_S3_BUCKET/$CODE_S3_BUCKET_KEY/$ZIP_FILE_NAME

function cfn_package_build {
  echo "Start cfn package"

  mkdir ${DIRNAME}/output
  aws aws cloudformation package --template-file ${TEMPLATE_PATH} --s3-bucket ${CODE_S3_BUCKET} --output-template-file ${BUNDLE_PATH} --capabilities CAPABILITY_IAM CAPABILITY_AUTO_EXPAND
  echo "Completed building Template"
}

function main{
    cfn_package_build
    code_zip

}
}
main
echo "Completed build stage"