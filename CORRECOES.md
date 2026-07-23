# Correções realizadas

- Substituição das conexões bloqueadas do Ricart–Agrawala por respostas `RA_DEFER` e concessões assíncronas `RA_GRANT`.
- Remoção automática de Syncs indisponíveis do conjunto de autorizações pendentes após falha confirmada pelo heartbeat.
- Redução dos tempos de detecção para evitar a impressão de congelamento.
- Inclusão de fencing token em cada entrada na seção crítica.
- Stores rejeitam escritas com fencing token antigo, protegendo contra Sync pausado que retorna com autorização expirada.
- Liberação da trava do item antes das RPCs W4/W5, evitando espera circular entre Stores.
- Ajuste dos testes para não depender do utilitário `bc` e para limitar o tempo das leituras externas.
