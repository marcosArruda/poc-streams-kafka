package br.com.marcosfariaarruda.empiricus.ordersservice.configs

import org.apache.kafka.streams.state.RocksDBConfigSetter
import org.rocksdb.Options
import kotlin.math.max

class CustomRocksDBConfig : RocksDBConfigSetter{
    override fun setConfig(storeName: String?, options: Options?, configs: MutableMap<String, Any>?) {
        val compactionParalellism = max(Runtime.getRuntime().availableProcessors(), 2)
        options?.setIncreaseParallelism(compactionParalellism)
        options?.setCreateIfMissing(true)
    }

}