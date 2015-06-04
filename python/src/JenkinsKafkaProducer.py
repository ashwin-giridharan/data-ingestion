import jenkins
import sys
from kafka import KafkaClient,SimpleProducer
j = jenkins.Jenkins('http://systest.jenkins.cloudera.com/', 'prayag.chandrannirmala', 'pErej2st')
jobs = j.get_info()['jobs']

for job in jobs:
  try:
    print '======================================='
    name = job['name']
    print name
    job_number = j.get_job_info(name)['lastSuccessfulBuild']['number']
    print name, job_number
    console_output = j.get_build_console_output(name, job_number)
    print '======================================='
    print console_output
    kafkaClient = KafkaClient("ashwin-centos65-1.vpc.cloudera.com:9092")
    kafkaProducer = SimpleProducer(kafkaClient)
    kafkaProducer.send_messages('ingesttest', console_output.encode('utf-8'))
  except TypeError as e:
    print "TypeError error({0})".format(e)
  except:
    print 'exception'
    print "Unexpected error:", sys.exc_info()[0]
    break



#j.get_job_info('CM-Systest-Create-And-Run')
#print j.get_build_console_output('CM-Systest-Create-And-Run',1354)