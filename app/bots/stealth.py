# =============================================================================
# STEALTH MODULE - Anti-détection avancée pour les bots
# =============================================================================

import random
from dataclasses import dataclass
from typing import Optional, List
from playwright.async_api import Browser, BrowserContext

# =============================================================================
# CONFIGURATION
# =============================================================================

# User Agents réalistes (Chrome sur Windows)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]

# Résolutions d'écran courantes
SCREEN_SIZES = [
    {"width": 1920, "height": 1080},
    {"width": 1680, "height": 1050},
    {"width": 1440, "height": 900},
    {"width": 1536, "height": 864},
    {"width": 2560, "height": 1440},
]

# Langues
LOCALES = ["fr-CH", "fr-FR", "de-CH", "en-US"]

# Timezones Suisse
TIMEZONES = ["Europe/Zurich", "Europe/Geneva"]

# =============================================================================
# STEALTH CONFIG
# =============================================================================

@dataclass
class StealthConfig:
    """Configuration pour un contexte stealth"""
    user_agent: str
    viewport: dict
    locale: str
    timezone: str
    proxy: Optional[dict] = None

def generate_stealth_config(proxy_url: Optional[str] = None) -> StealthConfig:
    """Génère une configuration stealth aléatoire"""
    config = StealthConfig(
        user_agent=random.choice(USER_AGENTS),
        viewport=random.choice(SCREEN_SIZES),
        locale=random.choice(LOCALES),
        timezone=random.choice(TIMEZONES)
    )
    
    if proxy_url:
        # Format: protocol://user:pass@host:port
        config.proxy = {"server": proxy_url}
    
    return config

# =============================================================================
# ANTI-DETECTION SCRIPTS
# =============================================================================

STEALTH_SCRIPTS = [
    # Masquer navigator.webdriver
    """
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined
    });
    """,
    
    # Masquer l'automatisation Chrome
    """
    window.chrome = {
        runtime: {},
        loadTimes: function() {},
        csi: function() {},
        app: {}
    };
    """,
    
    # Masquer les permissions
    """
    const originalQuery = window.navigator.permissions.query;
    window.navigator.permissions.query = (parameters) => (
        parameters.name === 'notifications' ?
            Promise.resolve({ state: Notification.permission }) :
            originalQuery(parameters)
    );
    """,
    
    # Plugins réalistes
    """
    Object.defineProperty(navigator, 'plugins', {
        get: () => [
            {name: 'Chrome PDF Plugin'},
            {name: 'Chrome PDF Viewer'},
            {name: 'Native Client'}
        ]
    });
    """,
    
    # Languages
    """
    Object.defineProperty(navigator, 'languages', {
        get: () => ['fr-CH', 'fr', 'en-US', 'en']
    });
    """,
    
    # Hardware concurrency
    """
    Object.defineProperty(navigator, 'hardwareConcurrency', {
        get: () => 8
    });
    """,
    
    # Device memory
    """
    Object.defineProperty(navigator, 'deviceMemory', {
        get: () => 8
    });
    """,
    
    # WebGL vendor
    """
    const getParameter = WebGLRenderingContext.prototype.getParameter;
    WebGLRenderingContext.prototype.getParameter = function(parameter) {
        if (parameter === 37445) return 'Intel Inc.';
        if (parameter === 37446) return 'Intel Iris OpenGL Engine';
        return getParameter.call(this, parameter);
    };
    """
]

# =============================================================================
# CONTEXT CREATION
# =============================================================================

async def get_stealth_context(
    browser: Browser,
    proxy_url: Optional[str] = None,
    config: Optional[StealthConfig] = None
) -> BrowserContext:
    """Crée un contexte de navigateur avec anti-détection"""
    
    if not config:
        config = generate_stealth_config(proxy_url)
    
    # Options du contexte
    context_options = {
        "user_agent": config.user_agent,
        "viewport": config.viewport,
        "locale": config.locale,
        "timezone_id": config.timezone,
        "permissions": ["geolocation"],
        "geolocation": {"latitude": 46.2044, "longitude": 6.1432},  # Genève
        "color_scheme": "light",
        "reduced_motion": "no-preference",
        "has_touch": False,
        "is_mobile": False,
        "device_scale_factor": 1,
    }
    
    # Ajouter le proxy si fourni
    if config.proxy:
        context_options["proxy"] = config.proxy
    
    # Créer le contexte
    context = await browser.new_context(**context_options)
    
    # Injecter les scripts anti-détection
    combined_script = "\n".join(STEALTH_SCRIPTS)
    await context.add_init_script(combined_script)
    
    return context

# =============================================================================
# HELPERS
# =============================================================================

def get_random_delay(min_sec: float = 1.0, max_sec: float = 3.0) -> float:
    """Retourne un délai aléatoire humanisé"""
    return random.uniform(min_sec, max_sec)

async def humanized_type(page, selector: str, text: str):
    """Tape du texte de manière humanisée"""
    element = await page.query_selector(selector)
    if element:
        await element.click()
        for char in text:
            await page.keyboard.type(char, delay=random.randint(50, 150))
            if random.random() < 0.1:  # 10% de chance de pause
                await page.wait_for_timeout(random.randint(200, 500))

async def humanized_scroll(page, direction: str = "down", amount: int = 300):
    """Scroll de manière humanisée"""
    steps = random.randint(3, 7)
    step_amount = amount // steps
    
    for _ in range(steps):
        if direction == "down":
            await page.mouse.wheel(0, step_amount)
        else:
            await page.mouse.wheel(0, -step_amount)
        await page.wait_for_timeout(random.randint(100, 300))

