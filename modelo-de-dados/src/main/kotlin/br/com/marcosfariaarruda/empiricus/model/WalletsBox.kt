package br.com.marcosfariaarruda.empiricus.model

import java.io.Serializable

class WalletsBox : Serializable {
    private var availableWallets = mutableMapOf<User?, Long?>()

    private val defaultInitMoney: Long = 10000

    fun createWallet(user: User): User {
        val newUser = user.copy(id = availableWallets.size.toLong())
        availableWallets[newUser] = defaultInitMoney
        return newUser
    }

    fun addCredits(user: User, credits: Long) {
        var currU = availableWallets.keys.find { onU -> onU?.name == user.name }
        if (currU == null) {
            currU = createWallet(user)
        }
        availableWallets[currU] = availableWallets[currU]?.plus(credits)
    }

    fun hasCredit(user: User, amount: Long): Boolean {
        var currU = availableWallets.keys.find { onU -> onU?.name == user.name }
        if (currU == null) {
            currU = createWallet(user)
        }
        val currVal: Long = availableWallets[currU] ?: return false
        return (currVal - amount) >= 0
    }

    fun transactDebit(user: User, amount: Long) {
        var currU = availableWallets.keys.find { onU -> onU?.name == user.name }
        if (currU == null) {
            currU = createWallet(user)
        }
        availableWallets[currU] = availableWallets[currU]?.minus(amount)
    }
}