package br.com.marcosfariaarruda.empiricus.model

import java.io.Serializable
import java.security.InvalidKeyException

class ProductBox : Serializable {
    private var DEFAULT_QUANTITY: Long = 50
    private var availableProductsMap = mutableMapOf<Product, Long?>(
            Pair(Product(0, "sapato", 11), DEFAULT_QUANTITY),
            Pair(Product(1, "celular", 22), DEFAULT_QUANTITY),
            Pair(Product(2, "curso", 33), DEFAULT_QUANTITY),
            Pair(Product(3, "abobrinha", 44), DEFAULT_QUANTITY),
            Pair(Product(4, "tenis", 55), DEFAULT_QUANTITY),
            Pair(Product(5, "sabão", 66), DEFAULT_QUANTITY),
            Pair(Product(6, "quadrado", 77), DEFAULT_QUANTITY),
            Pair(Product(7, "bola", 88), DEFAULT_QUANTITY)
    )

    fun stockAvailable(p: Product, quantity: Long): Boolean {
        val currP = availableProductsMap.keys.find { onP -> onP.name == p.name }
        return availableProductsMap[currP]!! >= quantity
    }

    fun buyIn(p: Product, quantity: Long): Boolean {
        if (stockAvailable(p, quantity)) {
            val currP = availableProductsMap.keys.find { onP -> onP.name == p.name }
                    ?: throw InvalidKeyException("Produto Invalido $p")
            availableProductsMap[currP] = availableProductsMap[currP]?.minus(quantity)
            return true
        }
        return false
    }

    fun getRandomProduct(): Product = availableProductsMap.keys.random()


    fun addNewProduct(p: Product, price: Int, quantity: Long) {
        availableProductsMap[p.copy(id = availableProductsMap.keys.size.toLong(), price = price)] = quantity
    }

    fun addMoreProducts(p: Product, quantity: Long) {
        val currP = availableProductsMap.keys.find { onP -> onP.name == p.name }
                ?: throw InvalidKeyException("Produto Invalido $p")
        availableProductsMap[currP] = availableProductsMap[currP]?.plus(quantity)
    }

    fun removeProduct(p: Product) {
        availableProductsMap.remove(availableProductsMap.keys.find { onP -> onP.name == p.name })
    }

    fun updatePriceProduct(p: Product, price: Int) {
        val currP = availableProductsMap.keys.find { onP -> onP.name == p.name }
                ?: throw InvalidKeyException("Produto Invalido $p")
        val pseudoNewP = currP.copy(price = price)
        availableProductsMap[pseudoNewP] = availableProductsMap[currP]
        availableProductsMap.remove(currP)
    }
}