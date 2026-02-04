# 📔 Guía de Gestión de Portafolio V2

Esta guía contiene los comandos necesarios para administrar tu portafolio (Ledger) desde la terminal del NAS.

## 🚀 Comandos Rápidos

Siempre ejecutar estos comandos desde la terminal de tu NAS (donde corre Docker).

### 1. Registrar una Compra (Histórica o Nueva)
```bash
sudo docker exec -it market_dashboard_v2 python3 tools/portfolio_cli.py add buy TICKER QTY PRICE --date "YYYY-MM-DD" --currency CCY
```
*Ejemplo real (FNOVA):*
```bash
sudo docker exec -it market_dashboard_v2 python3 tools/portfolio_cli.py add buy FNOVA17.MX 200 26.97 --date "2025-08-27" --currency MXN
```

### 2. Registrar una Venta
```bash
sudo docker exec -it market_dashboard_v2 python3 tools/portfolio_cli.py add sell TICKER QTY PRICE --date "YYYY-MM-DD"
```

### 3. Ver Estado Actual (Consolidado)
Muestra qué tienes hoy, cuánto tienes y a qué precio promedio lo compraste.
```bash
sudo docker exec -it market_dashboard_v2 python3 tools/portfolio_cli.py list
```

### 4. Ver Historial de un Activo (Ledger)
Muestra todas las transacciones pasadas de un ticker específico.
```bash
sudo docker exec -it market_dashboard_v2 python3 tools/portfolio_cli.py history --ticker FNOVA17.MX
```

---

## 🛠 Parámetros del Comando `add`

| Parámetro | Descripción | Ejemplo |
| :--- | :--- | :--- |
| `side` | Tipo de operación: `buy`, `sell` o `dividend`. | `buy` |
| `ticker` | Símbolo oficial (debe incluir `.MX` para BMV). | `NVDA` o `ALSEA.MX` |
| `qty` | Cantidad de títulos (número). | `100` |
| `price` | Precio unitario pagado/recibido. | `45.50` |
| `--date` | Opcional. Fecha de la operación (`YYYY-MM-DD`). | `--date "2025-01-20"` |
| `--currency` | Opcional. Moneda: `MXN` (default) o `USD`. | `--currency USD` |
| `--fees` | Opcional. Comisiones pagadas en la operación. | `--fees 15.20` |
| `--notes` | Opcional. Nota personal. | `--notes "Dip buy after earnings"` |

---

## 💡 Tips
- **Precios Promedio:** El sistema calcula el precio promedio automáticamente basándose en tus compras en el comando `list`.
- **Diferencia de Monedas:** Si registras en `USD`, el reporte lo marcará como tal.
- **Hot Reload:** No necesitas reiniciar el contenedor para que estos cambios surtan efecto; la DB es compartida.
