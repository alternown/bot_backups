import os
import sys
import asyncio
import struct
import logging
from datetime import datetime
from dotenv import load_dotenv

import discord
from discord import app_commands
from discord.ext import commands
from aiohttp import web, ClientSession, ClientTimeout

# Configuración de Logs local
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("PZBackupBot")

load_dotenv()

# Variables de entorno
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
DISCORD_GUILD_ID = os.getenv("DISCORD_GUILD_ID")

RCON_HOST = os.getenv("RCON_HOST", "45.236.90.225")
RCON_PORT = int(os.getenv("RCON_PORT", "26254"))
RCON_PASSWORD = os.getenv("RCON_PASSWORD", "")

PTERODACTYL_URL = os.getenv("PTERODACTYL_URL", "https://panel.rdsnode.com").rstrip('/')
PTERODACTYL_API_KEY = os.getenv("PTERODACTYL_API_KEY", "")
PTERODACTYL_SERVER_ID = os.getenv("PTERODACTYL_SERVER_ID", "341bcff3")

BACKUP_INTERVAL_HOURS = float(os.getenv("BACKUP_INTERVAL_HOURS", "6"))
LOG_CHANNEL_ID = int(os.getenv("LOG_CHANNEL_ID", "1539798378064642128"))

ADMIN_ROLE_IDS_RAW = os.getenv("ADMIN_ROLE_IDS", "")
ADMIN_ROLE_IDS = [int(r.strip()) for r in ADMIN_ROLE_IDS_RAW.split(",") if r.strip().isdigit()]

PORT = int(os.getenv("PORT", "10000"))

# Control global
backup_lock = asyncio.Lock()
last_backup_time = None
last_backup_status = "Ninguno"

# ==========================================
# PROTOCOLO SOURCE RCON (TCP)
# ==========================================
SERVERDATA_EXECCOMMAND = 2
SERVERDATA_AUTH = 3
SERVERDATA_AUTH_RESPONSE = 2

class SourceRCON:
    def __init__(self, host: str, port: int, password: str, timeout: float = 10.0):
        self.host = host
        self.port = port
        self.password = password
        self.timeout = timeout

    async def send_command(self, command: str) -> str:
        async with asyncio.timeout(self.timeout):
            reader, writer = await asyncio.open_connection(self.host, self.port)
            try:
                # Auth
                await self._send_packet(writer, 1, SERVERDATA_AUTH, self.password)
                req_id1, type1, _ = await self._read_packet(reader)
                req_id2, type2, _ = await self._read_packet(reader)

                if req_id2 == -1 or type2 != SERVERDATA_AUTH_RESPONSE:
                    raise Exception("Fallo de autenticación RCON: Contraseña incorrecta.")

                # Command
                await self._send_packet(writer, 2, SERVERDATA_EXECCOMMAND, command)
                req_id, p_type, body = await self._read_packet(reader)
                return body
            finally:
                writer.close()
                await writer.wait_closed()

    async def _send_packet(self, writer: asyncio.StreamWriter, req_id: int, p_type: int, body: str):
        body_bytes = body.encode('utf-8') + b'\x00\x00'
        size = 8 + len(body_bytes)
        packet = struct.pack('<iii', size, req_id, p_type) + body_bytes
        writer.write(packet)
        await writer.drain()

    async def _read_packet(self, reader: asyncio.StreamReader):
        size_data = await reader.readexactly(4)
        size = struct.unpack('<i', size_data)[0]
        packet_data = await reader.readexactly(size)
        
        req_id, p_type = struct.unpack('<ii', packet_data[:8])
        body = packet_data[8:-2].decode('utf-8', errors='replace')
        return req_id, p_type, body

async def run_rcon_command(cmd: str) -> str:
    rcon = SourceRCON(RCON_HOST, RCON_PORT, RCON_PASSWORD)
    return await rcon.send_command(cmd)

# ==========================================
# FUNCIONES DE PTERODACTYL API
# ==========================================
async def get_pterodactyl_backups() -> tuple[bool, list | str]:
    url = f"{PTERODACTYL_URL}/api/client/servers/{PTERODACTYL_SERVER_ID}/backups"
    headers = {
        "Authorization": f"Bearer {PTERODACTYL_API_KEY}",
        "Accept": "Application/vnd.pterodactyl.v1+json"
    }
    timeout = ClientTimeout(total=20)
    async with ClientSession(timeout=timeout) as session:
        try:
            async with session.get(url, headers=headers) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return True, data.get("data", [])
                else:
                    text = await resp.text()
                    return False, f"Error HTTP {resp.status}: {text}"
        except Exception as e:
            return False, f"Error de conexión: {str(e)}"

async def manage_pterodactyl_backups() -> tuple[bool, str]:
    base_url = f"{PTERODACTYL_URL}/api/client/servers/{PTERODACTYL_SERVER_ID}/backups"
    headers = {
        "Authorization": f"Bearer {PTERODACTYL_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "Application/vnd.pterodactyl.v1+json"
    }
    timeout = ClientTimeout(total=30)

    async with ClientSession(timeout=timeout) as session:
        try:
            # 1. Lista de backups
            async with session.get(base_url, headers=headers) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    return False, f"Error al consultar backups (HTTP {resp.status}): {text}"
                data = await resp.json()
                backups = data.get("data", [])

            # 2. Eliminar el más antiguo si hay 2 o más
            deleted_info = ""
            if len(backups) >= 2:
                backups_sorted = sorted(
                    backups, 
                    key=lambda b: b.get("attributes", {}).get("created_at", "")
                )
                oldest_backup = backups_sorted[0]["attributes"]
                oldest_uuid = oldest_backup["uuid"]
                oldest_name = oldest_backup.get("name", oldest_uuid)

                delete_url = f"{base_url}/{oldest_uuid}"
                async with session.delete(delete_url, headers=headers) as del_resp:
                    if del_resp.status in (200, 204):
                        logger.info(f"Backup eliminado: {oldest_name} ({oldest_uuid})")
                        deleted_info = f"\n🗑️ **Backup antiguo eliminado:** `{oldest_name}`"
                    else:
                        del_text = await del_resp.text()
                        return False, f"Error borrando backup antiguo `{oldest_uuid}` (HTTP {del_resp.status}): {del_text}"

            # 3. Crear nuevo backup
            async with session.post(base_url, headers=headers, json={}) as post_resp:
                post_data = await post_resp.json() if post_resp.content_type == 'application/json' else await post_resp.text()
                
                if post_resp.status in (200, 201, 202):
                    new_uuid = post_data.get('attributes', {}).get('uuid', 'Desconocido') if isinstance(post_data, dict) else "OK"
                    return True, f"✅ **Nuevo backup creado:** `{new_uuid}`{deleted_info}"
                else:
                    return False, f"Error al crear nuevo backup (HTTP {post_resp.status}): {post_data}"

        except asyncio.TimeoutError:
            return False, "Error de tiempo de espera al conectar con Pterodactyl."
        except Exception as e:
            return False, f"Excepción en Pterodactyl API: {str(e)}"

# ==========================================
# DISCORD BOT & EVENTOS
# ==========================================
intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

def is_admin(interaction: discord.Interaction) -> bool:
    if interaction.user.guild_permissions.administrator:
        return True
    if hasattr(interaction.user, 'roles'):
        user_role_ids = [role.id for role in interaction.user.roles]
        if any(r_id in ADMIN_ROLE_IDS for r_id in user_role_ids):
            return True
    return False

async def log_to_discord(title: str, success: bool, trigger_type: str, details: str, user: str = "Sistema"):
    global last_backup_time, last_backup_status
    if "Backup" in title:
        last_backup_time = datetime.now()
        last_backup_status = "Éxito" if success else "Error"

    channel = bot.get_channel(LOG_CHANNEL_ID)
    if not channel:
        logger.error(f"No se encontró el canal de logs ID: {LOG_CHANNEL_ID}")
        return

    color = discord.Color.green() if success else discord.Color.red()
    embed = discord.Embed(
        title=title,
        color=color,
        timestamp=datetime.now()
    )
    embed.add_field(name="Estado", value="✅ Exitoso" if success else "❌ Fallido", inline=True)
    embed.add_field(name="Origen / Usuario", value=f"{trigger_type} ({user})", inline=True)
    embed.add_field(name="Detalles", value=details[:1024], inline=False)
    embed.set_footer(text="Project Zomboid Control Panel")

    try:
        await channel.send(embed=embed)
    except Exception as e:
        logger.error(f"Error enviando mensaje a canal de logs: {e}")

async def execute_backup_workflow(trigger_type: str, user: str = "Sistema") -> tuple[bool, str]:
    if backup_lock.locked():
        return False, "Ya hay una copia de seguridad en curso."

    async with backup_lock:
        logger.info(f"Iniciando backup [{trigger_type}] por {user}...")
        
        try:
            await run_rcon_command('servermsg "ATENCION: Guardando mapa y realizando rotacion de copias de seguridad..."')
        except Exception as e:
            logger.warning(f"Aviso RCON inicial no enviado: {e}")

        try:
            await run_rcon_command("save")
        except Exception as e:
            msg = f"Error en RCON 'save': {e}"
            await log_to_discord("🛡️ Backup de Servidor", False, trigger_type, msg, user)
            return False, msg

        await asyncio.sleep(5)
        success, ptero_msg = await manage_pterodactyl_backups()

        try:
            if success:
                await run_rcon_command('servermsg "✅ Copia de seguridad completada con exito."')
            else:
                await run_rcon_command('servermsg "⚠️ Hubo un problema con la copia de seguridad. Contacte al admin."')
        except Exception as e:
            logger.warning(f"Aviso RCON final no enviado: {e}")

        await log_to_discord("🛡️ Backup de Servidor", success, trigger_type, ptero_msg, user)
        return success, ptero_msg

# ==========================================
# PANEL CONTROL: BOTONES INTERACTIVOS
# ==========================================
class ControlPanelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None) # Timeout=None para que los botones sean permanentes

    @discord.ui.button(label="Encender / Probar RCON", style=discord.ButtonStyle.success, custom_id="btn_power_rcon", emoji="🟢")
    async def btn_power_rcon(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message("❌ No tienes permisos.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        try:
            res = await run_rcon_command("players")
            msg = f"Conexión RCON exitosa.\n**Respuesta del servidor:** `{res.strip()}`"
            await log_to_discord("🟢 Test de Conexión RCON", True, "Panel de Control", msg, str(interaction.user))
            await interaction.followup.send(f"✅ RCON operativo.\nRespuesta: `{res.strip()}`", ephemeral=True)
        except Exception as e:
            msg = f"Error al conectar por RCON: {e}"
            await log_to_discord("🟢 Test de Conexión RCON", False, "Panel de Control", msg, str(interaction.user))
            await interaction.followup.send(f"❌ Error en la prueba RCON: {e}", ephemeral=True)

    @discord.ui.button(label="Revisar Backups", style=discord.ButtonStyle.primary, custom_id="btn_check_backups", emoji="📁")
    async def btn_check_backups(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message("❌ No tienes permisos.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        success, backups = await get_pterodactyl_backups()

        if not success:
            await log_to_discord("📁 Consulta de Backups", False, "Panel de Control", str(backups), str(interaction.user))
            await interaction.followup.send(f"❌ Error al consultar Pterodactyl: {backups}", ephemeral=True)
            return

        if not backups:
            details = "No hay copias de seguridad actualmente almacenadas en Pterodactyl."
        else:
            details_list = []
            for b in backups:
                attr = b.get("attributes", {})
                name = attr.get("name", "Sin nombre")
                uuid = attr.get("uuid", "N/A")
                bytes_size = attr.get("bytes", 0)
                mb_size = round(bytes_size / (1024 * 1024), 2)
                created = attr.get("created_at", "")[:19].replace("T", " ")
                details_list.append(f"• **{name}**\n  UUID: `{uuid}` | Tamaño: `{mb_size} MB` | Creado: `{created}`")
            details = "\n\n".join(details_list)

        await log_to_discord("📁 Consulta de Backups", True, "Panel de Control", f"Se consultaron {len(backups)} backups:\n\n{details}", str(interaction.user))
        await interaction.followup.send(f"📁 **Backups Actuales ({len(backups)}/2):**\n\n{details}", ephemeral=True)

    @discord.ui.button(label="Testing / Simulacro", style=discord.ButtonStyle.secondary, custom_id="btn_testing", emoji="🧪")
    async def btn_testing(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message("❌ No tienes permisos.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        
        # Simulacro
        test_steps = []
        # Step 1: Test RCON
        try:
            await run_rcon_command('servermsg "PRUEBA DE SISTEMA: Verificando conexion RCON..."')
            test_steps.append("1. RCON Avisos: OK")
        except Exception as e:
            test_steps.append(f"1. RCON Avisos: FALLO ({e})")

        # Step 2: Test Save
        try:
            await run_rcon_command('save')
            test_steps.append("2. RCON Save: OK")
        except Exception as e:
            test_steps.append(f"2. RCON Save: FALLO ({e})")

        # Step 3: Test API Pterodactyl Read
        p_success, backups = await get_pterodactyl_backups()
        if p_success:
            test_steps.append(f"3. API Pterodactyl: OK (Lectura exitosa, {len(backups)} backups encontrados)")
        else:
            test_steps.append(f"3. API Pterodactyl: FALLO ({backups})")

        summary = "\n".join(test_steps)
        overall_success = all("FALLO" not in s for s in test_steps)

        await log_to_discord("🧪 Simulacro de Sistema (Testing)", overall_success, "Panel de Control", summary, str(interaction.user))
        await interaction.followup.send(f"🧪 **Resultado del Simulacro:**\n\n{summary}", ephemeral=True)

    @discord.ui.button(label="Ejecutar Backup Manual", style=discord.ButtonStyle.danger, custom_id="btn_manual_backup", emoji="⚡")
    async def btn_manual_backup(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not is_admin(interaction):
            await interaction.response.send_message("❌ No tienes permisos.", ephemeral=True)
            return

        if backup_lock.locked():
            await interaction.response.send_message("⏳ Ya hay un proceso de backup en curso.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)
        success, msg = await execute_backup_workflow(trigger_type="Manual (Panel)", user=str(interaction.user))
        
        if success:
            await interaction.followup.send(f"✅ Backup manual finalizado con éxito.\n{msg}", ephemeral=True)
        else:
            await interaction.followup.send(f"❌ Error en backup manual:\n{msg}", ephemeral=True)

# ==========================================
# COMANDOS SLASH DISCORD
# ==========================================
@bot.tree.command(name="panel", description="Despliega el Panel de Control interactivo de Backups.")
async def cmd_panel(interaction: discord.Interaction):
    if not is_admin(interaction):
        await interaction.response.send_message("❌ No tienes permisos para usar este panel.", ephemeral=True)
        return

    embed = discord.Embed(
        title="🎛️ Panel de Control - Project Zomboid",
        description=(
            "Usa los botones a continuación para gestionar el servidor y las copias de seguridad.\n\n"
            "🟢 **Encender / Probar RCON:** Comprueba si el servidor responde comandos en juego.\n"
            "📁 **Revisar Backups:** Muestra la lista de respaldos almacenados.\n"
            "🧪 **Testing:** Realiza un diagnóstico de todos los módulos sin modificar archivos.\n"
            "⚡ **Ejecutar Backup Manual:** Genera un backup inmediato con rotación."
        ),
        color=discord.Color.gold()
    )
    embed.set_footer(text="Project Zomboid Backup Service")
    await interaction.response.send_message(embed=embed, view=ControlPanelView())

# Tarea programada
async def scheduled_backup_task():
    await bot.wait_until_ready()
    logger.info(f"Tarea programada activa cada {BACKUP_INTERVAL_HOURS} horas.")
    while not bot.is_closed():
        await asyncio.sleep(BACKUP_INTERVAL_HOURS * 3600)
        await execute_backup_workflow(trigger_type="Programado", user="Automático")

# ==========================================
# HEALTH CHECK WEBSERVER (RENDER)
# ==========================================
async def handle_health_check(request):
    return web.Response(text="Bot de Backup PZ activo", status=200)

async def start_web_server():
    app = web.Application()
    app.router.add_get('/', handle_health_check)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info(f"Servidor Web interno activo en puerto {PORT}")

# ==========================================
# RUN BOT
# ==========================================
@bot.event
async def on_ready():
    logger.info(f"Bot listo y conectado como: {bot.user.name}")
    # Registrar View persistente para que los botones sigan funcionando si el bot se reinicia
    bot.add_view(ControlPanelView())
    
    try:
        guild_obj = discord.Object(id=int(DISCORD_GUILD_ID)) if DISCORD_GUILD_ID else None
        if guild_obj:
            bot.tree.copy_global_to(guild=guild_obj)
            synced = await bot.tree.sync(guild=guild_obj)
            logger.info(f"Comandos registrados en el servidor: {len(synced)}")
        else:
            synced = await bot.tree.sync()
            logger.info(f"Comandos registrados globalmente: {len(synced)}")
    except Exception as e:
        logger.error(f"Error sincronizando comandos slash: {e}")

async def main():
    if not DISCORD_TOKEN:
        logger.critical("DISCORD_TOKEN no configurado en .env")
        return

    await start_web_server()
    asyncio.create_task(scheduled_backup_task())

    async with bot:
        await bot.start(DISCORD_TOKEN)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Bot detenido.")