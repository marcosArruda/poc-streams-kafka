package br.com.marcosfariaarruda.empiricus.ordersservice.services

import org.apache.kafka.streams.KafkaStreams
import org.springframework.context.ApplicationContext
import org.springframework.context.ApplicationContextAware
import org.springframework.stereotype.Service

@Service
class GlobalService : ApplicationContextAware {
    private lateinit var bigStreams: KafkaStreams
    private lateinit var applicationContext: ApplicationContext

    fun init(){

    }

    override fun setApplicationContext(applicationContext: ApplicationContext) {
        this.applicationContext = applicationContext
    }

    fun getApplicationContext():ApplicationContext{
        return this.applicationContext
    }

    /*
    fun myAnalysisStream(): Order? {
        streamAnalysis
                .join(streamOrders, ValueJoiner<OrderAnalysis, Order, Order> { analysisDone, originalOrder ->
                    originalOrder.analysis.add(analysisDone)
                    return@ValueJoiner originalOrder
                }, JoinWindows.of(Duration.ofMinutes(5)), Joined.with(Serdes.StringSerde(), OrderAnalysisSerde(), OrderSerde()))
    }
    */
}