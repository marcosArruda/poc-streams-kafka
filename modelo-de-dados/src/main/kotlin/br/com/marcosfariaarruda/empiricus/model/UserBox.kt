package br.com.marcosfariaarruda.empiricus.model

import java.io.Serializable
import kotlin.random.Random


class UserBox : Serializable {
    private var availableUsers:MutableMap<User, Long> = mutableMapOf<User, Long>(
            Pair(User(0, "Marcos", 34, mutableListOf()), 1),
            Pair(User(1, "Jonas", 37, mutableListOf()), 1),
            Pair(User(2, "Caio", 32, mutableListOf()), 1),
            Pair(User(3, "Matheus", 30, mutableListOf()), 1),
            Pair(User(4, "Rodrigo", 30, mutableListOf()), 1),
            Pair(User(5, "Michael", 30, mutableListOf()), 1),
            Pair(User(6, "Carolina", 30, mutableListOf()), 1),
            Pair(User(7, "Jotage", 30, mutableListOf()), 1),
            Pair(User(8, "Stag", 30, mutableListOf()), 1),
            Pair(User(9, "Igor", 30, mutableListOf()), 1),
            Pair(User(10, "Vicky", 30, mutableListOf()), 1)
    )

    fun getRandomUser(): User = availableUsers.keys.random()

    fun addUser(user: User) {
        availableUsers[user.copy(id = availableUsers.size.toLong())] = 1
    }

    fun deleteUser(user: User): Boolean {
        val currU = availableUsers.keys.find { onU -> onU.name == user.name }
        availableUsers.remove(currU) ?: return false
        return true
    }

    fun isFraud(order: Order): Boolean = order.isFraud

    private fun isPrime(num:Long?):Boolean{
        if(num == null || num <= 0L) return false
        val intNum = num.toInt()
        for (i in 2..(intNum/2)) { // condition for nonprime number
            if (intNum % i == 0) return true
        }
        return false
    }
}