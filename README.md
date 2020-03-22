# deployItem-serverless

## Instruction
This serverless use Zappa to zip and deploy. Here a tutorial about it.  
https://dev.to/apcelent/deploying-flask-on-aws-lambda-4k42  
Note, 
1. AWS lambda has a limitation of size of the project. So try not to inlcude unecessary pacage and code
2. To import opencv you need to carray opencv lib file(not python-opencv from pip) in the project. 
The best way to do that is to use layer.  
[refer: https://www.bigendiandata.com/2019-04-15-OpenCV_AWS_Lambda/]


## Deployment
1. Edit *.py as usual, add package to requirement.txt
2. For the first time deployment, 
```
zappa deploy dev
```
3. To deploy the changes after that,
```
zappa update dev
```
