set -u
bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

DIR=`cd $bin/../; pwd`
. "${DIR}/conf/configure.sh"

. "genSeedDataset.sh" $textdataset_recordsize_factor

echo "=========begin gen stream data========="
echo "Topic:$topic dataset:$app records:$records kafkaBrokers:$kafkabrokers mode:$mode"

if [ "$mode" == "push" ]; then
	records=$(($records/4))
	for ((i=0;i<4;i++)); do
		{
			java -Xmx256M -server -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark -XX:+DisableExplicitGC -Djava.awt.headless=true  -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false  -Dkafka.logs.dir=bin/../logs -cp :${DIR}/lib/kafka-clients-0.8.1.jar:${DIR}/target/datagen-0.0.1.jar com.intel.PRCcloud.StartNew $app $topic $kafkabrokers $records
		}& 
	done
	wait
else
	java -Xmx256M -server -XX:+UseCompressedOops -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled -XX:+CMSScavengeBeforeRemark -XX:+DisableExplicitGC -Djava.awt.headless=true  -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.authenticate=false  -Dcom.sun.management.jmxremote.ssl=false  -Dkafka.logs.dir=bin/../logs -cp :${DIR}/lib/kafka-clients-0.8.1.jar:${DIR}/target/datagen-0.0.1.jar com.intel.PRCcloud.StartPeriodic $app $topic $kafkabrokers $recordPerInterval $intervalSpan $totalRound
fi
