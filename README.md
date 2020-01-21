# Acta POC Streams

## O que é?

Este é o repositório da POC visando demonstrar o uso de Kafka para controlar o estado das entidades sem precisar persisti-las em um banco de dados local.

## Dependências

- Java 11
- Docker
- Docker Compose
- Spring WebFlux
- Spring Cloud-Streams
- Spring Cloud Kafka Binder

Obs.: [Documentação Kafka e Kafka Streams](https://docs.confluent.io/current/).

## Como rodar?

Basta chamar

```bash
root-dir$ ./compile-and-start-all.sh
```


## Macro Details

| Macro-épico | Funcionalidade |
|-------------|----------------|
| Gestão de pedidos | Upsell |
| Gestão de pedidos | Recomendação |
| Integração Externa | Salesforce |
| Gestão de contratos | Renovação |
| Gestão de contratos | Upgrade |
| Integração Externa | ERP Netsuite |
| Gestão de pagamentos | Métodos de pagamento |
| Gestão de pagamentos | Gestão de cartões |
| Gestão de pagamentos | Conta corrente |
| Gestão de pagamentos | Integração gateways de pagamento |
| Gestão de pagamentos | Anti-fraude |
| Gestão de pagamentos | Credit card 3Ds |
| Gestão de acessos  | Pedidos, contratos e cortesias |
| Gestão de catálogo  | Produto, plano e oferta |
| Gestão de catálogo  | Bonificação |
| Gestão de catálogo  | Degustação (tasting) |
| Gestão de catálogo  | Condições de pagamento |
| Gestão de catálogo  | Triggers e custom triggers |
| Gestão de catálogo  | Descontos globais |
| Gestão de catálogo  | Descontos de ciclo |
| Gestão de catálogo  | Upsell |
| Gestão de catálogo  | Inventário |
| Gestão de contratos | Bonificação |
| Gestão de contratos | Degustação |
| Gestão de contratos | Ações de contrato |
| Gestão de contratos  | Relacionamento entre contratos |
| Gestão de pagamentos | Estornos |
| Integração Externa | Disparador WS Marketing Cloud |
| Gestão de pedidos | Carrinhos |
| Gestão de pedidos | Controle de estoque/inventário |
| Logística | Controle de estoque/inventário |
| Logística | Tracking de pacotes |
| Reports | Relatórios |
| Gestão de contratos | Ações em lote |
| Gestão de clientes | Estrutura de clientes da plataforma |
| Gestão de pagamentos | Billing do SASS |

"Dúvidas:
- Teremos o accounts junto no SASS?
- Teremos publicações junto no SASS?
- Teremos um disparador de e-mails próprio para o SASS?
- Com quais gateways integraremos?
- Teremos um admin próprio para o SASS Multi-BU?" |
