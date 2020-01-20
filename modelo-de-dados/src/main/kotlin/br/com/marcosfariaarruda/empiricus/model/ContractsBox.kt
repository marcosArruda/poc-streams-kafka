package br.com.marcosfariaarruda.empiricus.model

import java.io.Serializable
import java.security.InvalidKeyException

class ContractsBox : Serializable {
    private var availableContracts = mutableMapOf<User, List<Contract>?>()

    fun createEmpty(user: User): User {
        val newUser = user.copy(id = availableContracts.size.toLong())
        availableContracts[newUser] = null
        return newUser
    }

    fun addContract(user: User, contract: Contract) {
        val currU = availableContracts.keys.find { onU -> onU.name == user.name } ?: throw InvalidKeyException("Usuario invalido $user")
        availableContracts[currU] = availableContracts[currU]?.plus(contract)
    }

    fun hasContract(user: User, contract: Contract): Boolean {
        val currU = availableContracts.keys.find { onU -> onU.name == user.name } ?: throw InvalidKeyException("Usuario invalido $user")
        val userContracts = availableContracts[currU] ?: return false
        if(userContracts.isEmpty()) return false
        userContracts.find { onC -> onC.id == contract.id } ?: return false
        return true
    }

    fun cancelContract(user:User, contract:Contract){
        val currU = availableContracts.keys.find { onU -> onU.name == user.name } ?: throw InvalidKeyException("Usuario invalido $user")
        val userContracts = availableContracts[currU] ?: throw InvalidKeyException("Contracto inexistente ou já removido $contract")
        if(userContracts.isEmpty()) throw InvalidKeyException("Contracto inexistente ou já removido $contract")
        val foundContract = userContracts.find { onC -> onC.id == contract.id } ?: throw InvalidKeyException("Contracto inexistente ou já removido $contract")
        userContracts.drop(userContracts.indexOf(foundContract))
    }
}