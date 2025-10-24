import os
import base64
from typing import Any, Optional, List, Dict

import httpx
from mcp.server.fastmcp import FastMCP

DATA_API_BASE = os.environ.get("DATA_API_BASE", "https://api.pre.iot.machinesensiot.com")

mcp = FastMCP("Buildot-Data-MCP")

# ---------- helpers ----------

def _need_token(bearer: Optional[str]):
    if not bearer:
        raise PermissionError("Missing bearer token (pass via gateway).")

def _client(bearer: str) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=DATA_API_BASE,
        headers={"Authorization": f"Bearer {bearer}"},
        timeout=httpx.Timeout(30.0, connect=10.0),
        http2=True,
        verify=False
    )

def _prune(d: Dict[str, Any]) -> Dict[str, Any]:
    """Drop None values (and empty lists) from dict for clean POST bodies."""
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, list) and len(v) == 0:
            continue
        out[k] = v
    return out

# ---------- meta ----------

@mcp.tool(description="MCP server health + which API base is configured")
async def health() -> dict:
    return {"ok": True, "api_base": DATA_API_BASE}

# ---------- Application ----------

@mcp.tool(description="List sites with optional search and pagination.")
async def get_sites(pageNo: int = 1, pageSize: int = 20, search: Optional[str] = None, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = _prune({"pageNo": pageNo, "pageSize": pageSize, "search": search})
    async with _client(bearer) as c:
        r = await c.post("/api/Application/GetAllSites", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List buildings, optionally filtered by siteId/buildingTypeId/search.")
async def get_buildings(pageNo: int = 1, pageSize: int = 20, search: Optional[str] = None,
                        siteId: Optional[int] = None, buildingTypeId: Optional[int] = None,
                        *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = _prune({"pageNo": pageNo, "pageSize": pageSize, "search": search,
                   "siteId": siteId, "buildingTypeId": buildingTypeId})
    async with _client(bearer) as c:
        r = await c.post("/api/Application/GetAllBuildings", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List locations with optional filters by siteIds/buildingIds/floorIds/name/search.")
async def get_locations(pageNo: int = 1, pageSize: int = 20, search: Optional[str] = None,
                        siteIds: Optional[List[int]] = None, buildingIds: Optional[List[int]] = None,
                        floorIds: Optional[List[int]] = None, name: Optional[str] = None,
                        *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = _prune({
        "pageNo": pageNo, "pageSize": pageSize, "search": search,
        "siteIds": siteIds, "buildingIds": buildingIds, "floorIds": floorIds, "name": name
    })
    async with _client(bearer) as c:
        r = await c.post("/api/Application/GetAllLocations", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="Concise building data with geo coordinates for maps.")
async def get_building_maps(*, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    async with _client(bearer) as c:
        r = await c.post("/api/Application/GetBuildingMaps")
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List building types (paginated).")
async def get_building_types(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Application/GetAllBuildingTypes", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List departments (paginated).")
async def get_departments(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Application/GetAllDepartments", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List applications (paginated).")
async def get_applications(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Application/GetAllApplications", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List application units (paginated).")
async def get_units(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Application/GetAllUnits", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List application attributes (paginated).")
async def get_attributes(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Application/GetAllAttributes", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List application groups (paginated).")
async def get_groups(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Application/GetAllGroups", json=body)
        r.raise_for_status()
        return r.json()

# ---------- Asset Management (Assets/Devices/Gateways) ----------

@mcp.tool(description="List asset type systems (top-level classes).")
async def get_asset_type_systems(pageNo: int = 1, pageSize: int = 20, name: Optional[str] = None, code: Optional[str] = None,
                                 *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = _prune({"pageNo": pageNo, "pageSize": pageSize, "name": name, "code": code})
    async with _client(bearer) as c:
        r = await c.post("/api/AssetManagement/GetAllAssetTypeSystems", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List asset types (2nd level classes under systems).")
async def get_asset_types(pageNo: int = 1, pageSize: int = 20, search: Optional[str] = None,
                          assetSystemIds: Optional[List[int]] = None, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = _prune({"pageNo": pageNo, "pageSize": pageSize, "search": search, "assetSystemIds": assetSystemIds})
    async with _client(bearer) as c:
        r = await c.post("/api/AssetManagement/GetAllAssetTypes", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List assets with filters (system/type/site/building/floor/location).")
async def get_assets(pageNo: int = 1, pageSize: int = 20, search: Optional[str] = None,
                     assetSystemIds: Optional[List[int]] = None, assetTypeIds: Optional[List[int]] = None,
                     siteIds: Optional[List[int]] = None, buildingIds: Optional[List[int]] = None,
                     floorIds: Optional[List[int]] = None, locationIds: Optional[List[int]] = None,
                     *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = _prune({
        "pageNo": pageNo, "pageSize": pageSize, "search": search,
        "assetSystemIds": assetSystemIds, "assetTypeIds": assetTypeIds,
        "siteIds": siteIds, "buildingIds": buildingIds, "floorIds": floorIds, "locationIds": locationIds
    })
    async with _client(bearer) as c:
        r = await c.post("/api/AssetManagement/GetAllAssets", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List devices; supports filtering by groupIds/siteIds/buildingIds/floorIds/applicationIds.")
async def get_devices(pageNo: int = 1, pageSize: int = 20, search: Optional[str] = None,
                      groupIds: Optional[List[int]] = None, siteIds: Optional[List[int]] = None,
                      buildingIds: Optional[List[int]] = None, floorIds: Optional[List[int]] = None,
                      applicationIds: Optional[List[int]] = None, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = _prune({
        "pageNo": pageNo, "pageSize": pageSize, "search": search,
        "groupIds": groupIds, "siteIds": siteIds, "buildingIds": buildingIds,
        "floorIds": floorIds, "applicationIds": applicationIds
    })
    async with _client(bearer) as c:
        r = await c.post("/api/AssetManagement/GetAllDevices", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List gateways with optional filters (site/building/floor/location).")
async def get_gateways(pageNo: int = 1, pageSize: int = 20, search: Optional[str] = None,
                       siteIds: Optional[List[int]] = None, buildingIds: Optional[List[int]] = None,
                       floorIds: Optional[List[int]] = None, locationIds: Optional[List[int]] = None,
                       *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = _prune({
        "pageNo": pageNo, "pageSize": pageSize, "search": search,
        "siteIds": siteIds, "buildingIds": buildingIds, "floorIds": floorIds, "locationIds": locationIds
    })
    async with _client(bearer) as c:
        r = await c.post("/api/AssetManagement/GetAllGateways", json=body)
        r.raise_for_status()
        return r.json()

# ---------- Solutions (read/write) ----------

@mcp.tool(description="Snapshot of latest values for devices under an application.")
async def get_solutions_by_application(applicationId: int, pageNo: int = 1, pageSize: int = 20, *,
                                       bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize, "applicationId": applicationId}
    async with _client(bearer) as c:
        r = await c.post("/api/Solution/GetSolutionsByApplication", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="Send attributes to a solution/device (write).")
async def send_solution_data(solutionId: int, attributes: List[Dict[str, str]], *,
                             bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"solutionId": solutionId, "attributes": attributes}
    async with _client(bearer) as c:
        r = await c.post("/api/Solution/SendSolutionData", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="Get climate solution (map + latest values) for a floor.")
async def get_climate_solution_by_floor(floorId: int, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    async with _client(bearer) as c:
        r = await c.get("/api/Solution/GetClimateSolutionByFloor", params={"floorId": floorId})
        r.raise_for_status()
        return r.json()

# ---------- Widgets / Asset Dashboards ----------

@mcp.tool(description="Get widget template for an application.")
async def get_widget_template_by_application(applicationId: int, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    async with _client(bearer) as c:
        r = await c.get("/api/Widget/GetWidgetTemplateByApplication", params={"applicationId": applicationId})
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List all widget templates (paginated).")
async def get_all_widget_templates(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Widget/GetAllWidgetTemplates", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="Get widget data for a device in an application.")
async def get_widget_data(applicationId: int, deviceId: int, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"applicationId": applicationId, "deviceId": deviceId}
    async with _client(bearer) as c:
        r = await c.post("/api/Widget/GetWidgetData", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List icon keys.")
async def get_icon_names(*, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    async with _client(bearer) as c:
        r = await c.get("/api/Widget/GetIconNames")
        r.raise_for_status()
        return r.json()

@mcp.tool(description="Fetch an icon image by key. Returns base64 + content_type.")
async def get_icon_image(key: str, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    async with _client(bearer) as c:
        r = await c.get("/api/Widget/GetIconImage", params={"key": key})
        r.raise_for_status()
        content_type = r.headers.get("content-type", "application/octet-stream")
        b64 = base64.b64encode(r.content).decode("utf-8")
        return {"content_type": content_type, "base64": b64}

@mcp.tool(description="List all asset templates.")
async def get_all_asset_templates(*, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    async with _client(bearer) as c:
        r = await c.get("/api/Widget/GetAllAssetTemplates")
        r.raise_for_status()
        return r.json()

@mcp.tool(description="Get asset dashboard by assetId.")
async def get_asset_dashboard(assetId: int, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    async with _client(bearer) as c:
        r = await c.get("/api/Widget/GetAssetDashboard", params={"assetId": assetId})
        r.raise_for_status()
        return r.json()

# ---------- Unit Prices ----------

@mcp.tool(description="List all unit prices (paginated).")
async def get_all_unit_prices(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/UnitPrice/GetAllUnitPrices", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="Get unit price for a specific location and utilityType.")
async def get_unit_price_by_location(locationId: int, utilityType: int, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    # The spec uses POST with query params and empty body
    async with _client(bearer) as c:
        r = await c.post("/api/UnitPrice/GetUnitPriceByLocation",
                         params={"locationId": locationId, "utilityType": utilityType}, json={})
        r.raise_for_status()
        return r.json()

@mcp.tool(description="Get unit prices for a building + utility type (paginated).")
async def get_unit_prices_by_building(buildingId: int, utilityType: int,
                                      pageNo: int = 1, pageSize: int = 20, *,
                                      bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize, "buildingId": buildingId, "utilityType": utilityType}
    async with _client(bearer) as c:
        r = await c.post("/api/UnitPrice/GetUnitPricesByBuilding", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="Get unit prices for a floor + utility type (paginated).")
async def get_unit_prices_by_floor(floorId: int, utilityType: int,
                                   pageNo: int = 1, pageSize: int = 20, *,
                                   bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize, "floorId": floorId, "utilityType": utilityType}
    async with _client(bearer) as c:
        r = await c.post("/api/UnitPrice/GetUnitPricesByFloor", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List all currencies (paginated).")
async def get_all_currencies(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/UnitPrice/GetAllCurrencies", json=body)
        r.raise_for_status()
        return r.json()

# ---------- Notifications ----------

@mcp.tool(description="Get notifications (filters + pagination).")
async def get_notifications(pageNo: int = 1, pageSize: int = 20,
                            assetIds: Optional[List[int]] = None, siteIds: Optional[List[int]] = None,
                            buildingIds: Optional[List[int]] = None, applicationIds: Optional[List[int]] = None,
                            sourceType: Optional[int] = None,
                            startDate: Optional[str] = None, endDate: Optional[str] = None,
                            status: Optional[List[int]] = None, type: Optional[List[int]] = None,
                            priority: Optional[List[int]] = None, searchText: Optional[str] = None,
                            section: Optional[str] = None, viewMode: Optional[str] = None,
                            *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = _prune({
        "pageNo": pageNo, "pageSize": pageSize, "assetIds": assetIds, "siteIds": siteIds,
        "buildingIds": buildingIds, "applicationIds": applicationIds, "sourceType": sourceType,
        "startDate": startDate, "endDate": endDate, "status": status, "type": type,
        "priority": priority, "searchText": searchText, "section": section, "viewMode": viewMode
    })
    async with _client(bearer) as c:
        r = await c.post("/api/Notification/GetAllNotifications", json=body)
        r.raise_for_status()
        return r.json()

# ---------- Scheduler ----------

@mcp.tool(description="List schedule rules (paginated).")
async def get_all_schedule_rules(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Scheduler/GetAllScheduleRules", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List device schedules (paginated).")
async def get_all_device_schedules(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Scheduler/GetAllDeviceSchedules", json=body)
        r.raise_for_status()
        return r.json()

# ---------- Checklists ----------

@mcp.tool(description="List all checklists (paginated).")
async def get_all_checklists(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Checklist/GetAllChecklists", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List all checklist types (paginated).")
async def get_all_checklist_types(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Checklist/GetAllChecklistTypes", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="Get my (current user) checklists (paginated).")
async def get_my_checklists(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Checklist/GetMyChecklists", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List all organization checklists, optional filters.")
async def get_all_org_checklists(pageNo: int = 1, pageSize: int = 20,
                                 search: Optional[str] = None, checklistTypeId: Optional[int] = None,
                                 *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = _prune({"pageNo": pageNo, "pageSize": pageSize, "search": search, "checklistTypeId": checklistTypeId})
    async with _client(bearer) as c:
        r = await c.post("/api/Checklist/GetAllOrganizationChecklists", json=body)
        r.raise_for_status()
        return r.json()

# ---------- Team Members ----------

@mcp.tool(description="List all team members (paginated).")
async def get_team_members(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/TeamMember/GetAllTeamMember", json=body)
        r.raise_for_status()
        return r.json()

# ---------- Sustainability ----------

@mcp.tool(description="Get energy dashboard data (sites/buildings/year/month filters).")
async def get_energy_dashboard_data(siteIds: Optional[List[int]] = None, buildingIds: Optional[List[int]] = None,
                                    year: int = 0, month: int = 0, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = _prune({"siteIds": siteIds, "buildingIds": buildingIds, "year": year, "month": month})
    async with _client(bearer) as c:
        r = await c.post("/api/Sustainability/GetEnergyDashboardData", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List sustainability factors (paginated).")
async def get_all_sustainability_factors(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Sustainability/GetAllSustainabilityFactors", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List sustainability frameworks (paginated).")
async def get_all_sustainability_frameworks(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Sustainability/GetAllSustainabilityFrameworks", json=body)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="List sustainability categories (paginated).")
async def get_all_sustainability_categories(pageNo: int = 1, pageSize: int = 20, *, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    body = {"pageNo": pageNo, "pageSize": pageSize}
    async with _client(bearer) as c:
        r = await c.post("/api/Sustainability/GetAllSustainabilityCategory", json=body)
        r.raise_for_status()
        return r.json()

def _clean_payload(d: dict) -> dict:
    return {k: v for k, v in d.items() if v is not None}


# ----- Energy (Dashboard) -----

@mcp.tool(description="Proxy for /api/Dashboard/GetOverviewDashboard")
async def get_overview_dashboard(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetOverviewDashboard."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetOverviewDashboard", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetDailyUtilityConsumption")
async def get_daily_utility_consumption(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetDailyUtilityConsumption."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetDailyUtilityConsumption", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetIndoorAiqRankings")
async def get_indoor_aiq_rankings(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetIndoorAiqRankings."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetIndoorAiqRankings", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetUtilityConsumptionMetrics")
async def get_utility_consumption_metrics(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetUtilityConsumptionMetrics."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetUtilityConsumptionMetrics", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetBuildingTopPerforming")
async def get_building_top_performing(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetBuildingTopPerforming."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetBuildingTopPerforming", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetScope2UtilityConsumptionMetrics")
async def get_scope2_utility_consumption_metrics(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetScope2UtilityConsumptionMetrics."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetScope2UtilityConsumptionMetrics", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetEnergyAssetConsumption")
async def get_energy_asset_consumption(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetEnergyAssetConsumption."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetEnergyAssetConsumption", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetDailyEnergyCostBySitesBuildings")
async def get_daily_energy_cost_by_sites_buildings(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetDailyEnergyCostBySitesBuildings."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetDailyEnergyCostBySitesBuildings", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetMonthlyEnergyMetrics")
async def get_monthly_energy_metrics(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetMonthlyEnergyMetrics."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetMonthlyEnergyMetrics", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetDailyEnergyConsumptionBySitesBuildings")
async def get_daily_energy_consumption_by_sites_buildings(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetDailyEnergyConsumptionBySitesBuildings."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetDailyEnergyConsumptionBySitesBuildings", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetComparativeMonthlyEnergyConsumptionBySitesBuildings")
async def get_comparative_monthly_energy_consumption_by_sites_buildings(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetComparativeMonthlyEnergyConsumptionBySitesBuildings."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetComparativeMonthlyEnergyConsumptionBySitesBuildings", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetComparativeMonthlyEnergyCostBySitesBuildings")
async def get_comparative_monthly_energy_cost_by_sites_buildings(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetComparativeMonthlyEnergyCostBySitesBuildings."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetComparativeMonthlyEnergyCostBySitesBuildings", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetDeviceEnergyConsumptionBySitesBuildings")
async def get_device_energy_consumption_by_sites_buildings(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetDeviceEnergyConsumptionBySitesBuildings."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetDeviceEnergyConsumptionBySitesBuildings", json=payload)
        r.raise_for_status()
        return r.json()


# ----- BTU -----

@mcp.tool(description="Proxy for /api/Dashboard/GetMonthlyBtuMetrics")
async def get_monthly_btu_metrics(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetMonthlyBtuMetrics."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetMonthlyBtuMetrics", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetDeviceBtuCostBySitesBuildings")
async def get_device_btu_cost_by_sites_buildings(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetDeviceBtuCostBySitesBuildings."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetDeviceBtuCostBySitesBuildings", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetDailyBtuConsumptionBySitesBuildings")
async def get_daily_btu_consumption_by_sites_buildings(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetDailyBtuConsumptionBySitesBuildings."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetDailyBtuConsumptionBySitesBuildings", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetBuildingBtuConsumptionBySitesBuildings")
async def get_building_btu_consumption_by_sites_buildings(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetBuildingBtuConsumptionBySitesBuildings."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetBuildingBtuConsumptionBySitesBuildings", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetBuildingBtuCostBySitesBuildings")
async def get_building_btu_cost_by_sites_buildings(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetBuildingBtuCostBySitesBuildings."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetBuildingBtuCostBySitesBuildings", json=payload)
        r.raise_for_status()
        return r.json()


# ----- Water -----

@mcp.tool(description="Proxy for /api/Dashboard/GetMonthlyWaterMetrics")
async def get_monthly_water_metrics(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetMonthlyWaterMetrics."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetMonthlyWaterMetrics", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetDailyWaterConsumptionBySitesBuildings")
async def get_daily_water_consumption_by_sites_buildings(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetDailyWaterConsumptionBySitesBuildings."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetDailyWaterConsumptionBySitesBuildings", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetDeviceWaterConsumptionBySitesBuildings")
async def get_device_water_consumption_by_sites_buildings(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetDeviceWaterConsumptionBySitesBuildings."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetDeviceWaterConsumptionBySitesBuildings", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetComparativeMonthlyWaterConsumptionBySitesBuildings")
async def get_comparative_monthly_water_consumption_by_sites_buildings(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetComparativeMonthlyWaterConsumptionBySitesBuildings."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetComparativeMonthlyWaterConsumptionBySitesBuildings", json=payload)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="Proxy for /api/Dashboard/GetComparativeMonthlyWaterCostBySitesBuildings")
async def get_comparative_monthly_water_cost_by_sites_buildings(
    pageNo: int = 1,
    pageSize: int = 20,
    search: Optional[str] = None,
    siteIds: Optional[list[int]] = None,
    buildingIds: Optional[list[int]] = None,
    floorIds: Optional[list[int]] = None,
    applicationIds: Optional[list[int]] = None,
    assetIds: Optional[list[int]] = None,
    startDate: Optional[str] = None,
    endDate: Optional[str] = None,
    extra: Optional[dict] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    """Call /api/Dashboard/GetComparativeMonthlyWaterCostBySitesBuildings."""
    _need_token(bearer)
    payload = _clean_payload(
        {
            "pageNo": pageNo,
            "pageSize": pageSize,
            "search": search,
            "siteIds": siteIds,
            "buildingIds": buildingIds,
            "floorIds": floorIds,
            "applicationIds": applicationIds,
            "assetIds": assetIds,
            "startDate": startDate,
            "endDate": endDate,
        }
    )
    if extra:
        payload.update(extra)
    async with _client(bearer) as c:
        r = await c.post("/api/Dashboard/GetComparativeMonthlyWaterCostBySitesBuildings", json=payload)
        r.raise_for_status()
        return r.json()
# =========================
# EMS (Energy) API - tools
# =========================

# Base for EMS endpoints (override via env if needed)
EMS_API_BASE = os.environ.get("EMS_API_BASE", "https://energy.machinesensiot.com")

def _client_ems(bearer: str) -> httpx.AsyncClient:
    """
    EMS HTTP client – same pattern as _client(), but pointing to EMS base.
    Always sends Bearer and Referer as per examples.
    """
    return httpx.AsyncClient(
        base_url=EMS_API_BASE,
        headers={
            "Authorization": f"Bearer {bearer}",
            "Referer": "https://energy.machinesensiot.com/Portfolio/Index?org_id=83&user_id=14",
        },
        timeout=httpx.Timeout(30.0, connect=10.0),
        http2=True,
        verify=False,
    )

def _q(d: Dict[str, Any]) -> Dict[str, Any]:
    """Prune None/empty list values for clean query params (GET)."""
    out: Dict[str, Any] = {}
    for k, v in d.items():
        if v is None:
            continue
        if isinstance(v, list) and len(v) == 0:
            continue
        out[k] = v
    return out

from typing import Optional, Any

# ---------- Portfolio (Sites / Building Types / Water / Electricity) ----------

@mcp.tool(description="EMS Portfolio/GetSites – List all sites in the portfolio.")
async def ems_portfolio_get_sites(*, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetSites")
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS Portfolio/GetBuildingTypes – List of building types.")
async def ems_portfolio_get_building_types(*, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetBuildingTypes")
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS Portfolio/GetWaterSites – Sites with water data.")
async def ems_portfolio_get_water_sites(*, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetWaterSites")
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS Portfolio/GetChilledWaterSites – Sites with chilled water data.")
async def ems_portfolio_get_chilled_water_sites(*, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetChilledWaterSites")
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS Portfolio/GetUtilityEnergyStats – Summary stats (voltage, current, power, PF, freq, total energy).")
async def ems_portfolio_get_utility_energy_stats(
    startDate: str,
    endDate: str,
    siteId: Optional[int] = 0,
    buildingId: Optional[int] = 0,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({
        "siteId": siteId,
        "buildingId": buildingId,
        "startDate": startDate,
        "endDate": endDate
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetUtilityEnergyStats", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS Portfolio/GetElectricityConsumption – Electricity consumption for the date range; optional site/building filters.")
async def ems_portfolio_get_electricity_consumption(
    startDate: str,
    endDate: str,
    siteId: Optional[int] = None,
    buildingId: Optional[int] = None,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "siteId": siteId,
        "buildingId": buildingId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetElectricityConsumption", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS Portfolio/GetElectricityConsumptionHeatMap – Daily electricity consumption heatmap.")
async def ems_portfolio_get_electricity_consumption_heatmap(
    startDate: str,
    endDate: str,
    siteId: Optional[int] = None,
    buildingTypeId: Optional[int] = None,
    buildingId: Optional[int] = None,
    meterId: Optional[int] = None,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "siteId": siteId,
        "buildingTypeId": buildingTypeId,
        "buildingId": buildingId,
        "meterId": meterId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetElectricityConsumptionHeatMap", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS Portfolio/GetElectricityPieChart – Electricity consumption grouped by building type for the months in range.")
async def ems_portfolio_get_electricity_pie_chart(
    startDate: str,
    endDate: str,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetElectricityPieChart", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS Portfolio/GetElectricityCost – Electricity cost per building for the date range.")
async def ems_portfolio_get_electricity_cost(
    startDate: str,
    endDate: str,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetElectricityCost", params=params)
        r.raise_for_status()
        return r.json()


# ---------- EnergyPerformanceAnalysis (EPA) ----------

@mcp.tool(description="EMS EPA/GetEnergyStats – Key energy stats (current/prev month, per-day avg, cost, intensity).")
async def ems_epa_get_energy_stats(
    startDate: str,
    endDate: str,
    siteId: Optional[int] = None,
    buildingId: Optional[int] = None,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({
        "siteId": siteId,
        "buildingId": buildingId,
        "startDate": startDate,
        "endDate": endDate
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyPerformanceAnalysis/GetEnergyStats", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS EPA/GetBuildingTypes – Building types (EPA context).")
async def ems_epa_get_building_types(*, bearer: Optional[str] = None) -> Any:
    _need_token(bearer)
    # Same endpoint as portfolio building types; kept separate name for clarity if UI needs context.
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetBuildingTypes")
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS EPA (Portfolio path)/GetElectricityConsumption – Electricity consumption with EPA context; optional site/building.")
async def ems_epa_get_electricity_consumption(
    startDate: str,
    endDate: str,
    siteId: Optional[int] = None,
    buildingId: Optional[int] = None,
    *,
    bearer: Optional[str] = None
) -> Any:
    # Endpoint path remains under /Portfolio; separate tool name for EPA flows that expect it.
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "siteId": siteId,
        "buildingId": buildingId
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetElectricityConsumption", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS EPA (Portfolio path)/GetElectricityConsumptionHeatMap – Heatmap with EPA context; optional site/building/meter filters.")
async def ems_epa_get_electricity_consumption_heatmap(
    startDate: str,
    endDate: str,
    siteId: Optional[int] = None,
    buildingTypeId: Optional[int] = None,
    buildingId: Optional[int] = None,
    meterId: Optional[int] = None,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "siteId": siteId,
        "buildingTypeId": buildingTypeId,
        "buildingId": buildingId,
        "meterId": meterId
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetElectricityConsumptionHeatMap", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS EPA/GetTotalElectricityCostByBuildingSite – Site + building level electricity cost for period.")
async def ems_epa_get_total_electricity_cost_by_building_site(
    startDate: str,
    endDate: str,
    siteId: Optional[int] = None,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyPerformanceAnalysis/GetTotalElectricityCostByBuildingSite", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS EPA/GetTotalEnergyConsumptionByBuilding – Total energy consumption per building.")
async def ems_epa_get_total_energy_consumption_by_building(
    startDate: str,
    endDate: str,
    siteId: Optional[int] = None,
    buildingId: Optional[int] = None,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyPerformanceAnalysis/GetTotalEnergyConsumptionByBuilding", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS EPA/GetAverageUsageIntensity – EUI per building/time period.")
async def ems_epa_get_average_usage_intensity(
    startDate: str,
    endDate: str,
    siteId: Optional[int] = None,
    buildingId: Optional[int] = None,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyPerformanceAnalysis/GetAverageUsageIntensity", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS EPA/GetBuildingWiseEnergyUsageIntensityPercentage – EUI and percentage per building.")
async def ems_epa_get_building_wise_energy_usage_intensity_percentage(
    startDate: str,
    endDate: str,
    siteId: Optional[int] = None,
    buildingId: Optional[int] = None,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyPerformanceAnalysis/GetBuildingWiseEnergyUsageIntensityPercentage", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS EPA/GetBuildingandYearWiseEui – Year-wise EUI by building/month.")
async def ems_epa_get_building_and_year_wise_eui(
    startDate: str,
    endDate: str,
    siteId: Optional[int] = None,
    buildingId: Optional[int] = None,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyPerformanceAnalysis/GetBuildingandYearWiseEui", params=params)
        r.raise_for_status()
        return r.json()


# ---------- Sustainability Insights ----------

@mcp.tool(description="EMS SustainabilityInsights/GetBuildingEnergyPortfolioData – Energy by building with proportional contribution and top performers.")
async def ems_sustainability_get_building_energy_portfolio_data(
    startDate: str,
    endDate: str,
    siteId: Optional[int] = None,
    buildingName: Optional[str] = None,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "siteId": siteId,
        "buildingName": buildingName
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/SustainabilityInsights/GetBuildingEnergyPortfolioData", params=params)
        r.raise_for_status()
        return r.json()


# ---------- Benchmarking ----------

@mcp.tool(description="EMS BenchMarking/GetSiteLevelEnergyCostData – Site-level monthly energy, cost, and intensities.")
async def ems_benchmarking_get_site_level_energy_cost_data(
    startDate: str,
    endDate: str,
    sites: Optional[str] = None,  # comma-separated IDs, or '0' for all
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"sites": sites, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelEnergyCostData", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS UnitConversion/EnergyCarbonFootPrintTrendOverTime – Carbon footprint trend over time with energy + CO₂, by site/building.")
async def ems_unit_conversion_energy_carbon_footprint_trend_over_time(
    startDate: str,
    endDate: str,
    siteId: Optional[int] = None,
    buildingId: Optional[int] = None,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"startDate": startDate, "endDate": endDate, "siteId": siteId, "buildingId": buildingId})
    async with _client_ems(bearer) as c:
        r = await c.get("/UnitConversion/EnergyCarbonFootPrintTrendOverTime", params=params)
        r.raise_for_status()
        return r.json()


# ---------- Benchmarking (site-level emissions/energy) ----------

@mcp.tool(description="EMS BenchMarking/GetSiteLevelCarbonEmissionPerformance – Carbon emission performance by site (emissions, intensity, category).")
async def ems_benchmarking_get_site_level_carbon_emission_performance(
    startDate: str,
    endDate: str,
    sites: Optional[str] = None,  # comma-separated IDs or '0' for all
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"sites": sites, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelCarbonEmissionPerformance", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS BenchMarking/GetSitesLevelBenchmarkData – Actual vs benchmark consumption with deviation% per site/building.")
async def ems_benchmarking_get_sites_level_benchmark_data(
    startDate: str,
    endDate: str,
    sites: Optional[str] = None,  # comma-separated IDs or '0' for all
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"sites": sites, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSitesLevelBenchmarkData", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS BenchMarking/GetSiteLevelCarbonDeviationAnalysis – Deviation of actual vs best-practice carbon by site.")
async def ems_benchmarking_get_site_level_carbon_deviation_analysis(
    startDate: str,
    endDate: str,
    sites: Optional[str] = None,  # comma-separated IDs or '0' for all
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"sites": sites, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelCarbonDeviationAnalysis", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS BenchMarking/GetSiteLevelEnergyAndCarbonIntensity – Energy & carbon intensity by site/month (POST form).")
async def ems_benchmarking_get_site_level_energy_and_carbon_intensity(
    startDate: str,
    endDate: str,
    sites: Optional[str] = None,  # comma-separated IDs or '0' for all
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    form = _q({"sites": sites, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.post("/BenchMarking/GetSiteLevelEnergyAndCarbonIntensity", data=form)
        r.raise_for_status()
        return r.json()


# ---------- Sustainability Insights (lookup helpers) ----------

@mcp.tool(description="EMS SustainabilityInsights/GetBuildingsBySite – List buildings for a given siteId.")
async def ems_sustainability_get_buildings_by_site(
    siteId: int,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId})
    async with _client_ems(bearer) as c:
        r = await c.get("/SustainabilityInsights/GetBuildingsBySite", params=params)
        r.raise_for_status()
        return r.json()


# ---------- Time Profiling (site/building profiling / trends) ----------

@mcp.tool(description="EMS TimeProfiling/GetMonthlyEnergyByBuilding – Monthly energy by building for a site.")
async def ems_timeprofiling_get_monthly_energy_by_building(
    siteName: str,
    startDate: str,
    endDate: str,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"siteName": siteName, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetMonthlyEnergyByBuilding", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS TimeProfiling/GetTotalSiteEnergyByTimeOfUse – Site total energy by TOU (peak/off-peak/etc.).")
async def ems_timeprofiling_get_total_site_energy_by_time_of_use(
    siteId: int,
    startDate: str,
    endDate: str,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetTotalSiteEnergyByTimeOfUse", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS TimeProfiling/GetDailyConsumptionByWeekday – Avg daily kWh by weekday for a site.")
async def ems_timeprofiling_get_daily_consumption_by_weekday(
    siteId: int,
    startDate: str,
    endDate: str,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetDailyConsumptionByWeekday", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS TimeProfiling/GetDailyEnergyIntensityByWeekday – Avg daily energy intensity (kWh/m²) by weekday.")
async def ems_timeprofiling_get_daily_energy_intensity_by_weekday(
    siteId: int,
    startDate: str,
    endDate: str,
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetDailyEnergyIntensityByWeekday", params=params)
        r.raise_for_status()
        return r.json()


# ---------- Benchmarking (profiling/impacts/trends) ----------

@mcp.tool(description="EMS BenchMarking/GetStartupShutdownEnergyProfiling – Hourly startup/shutdown profiling with operational insights.")
async def ems_benchmarking_get_startup_shutdown_energy_profiling(
    siteId: int,
    startDate: str,
    endDate: str,
    buildingName: Optional[str] = None,  # '0' for all buildings if needed
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingName": buildingName, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetStartupShutdownEnergyProfiling", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS BenchMarking/GetHighDemandHoursByBuilding – High-demand hour detection per building.")
async def ems_benchmarking_get_high_demand_hours_by_building(
    siteId: int,
    startDate: str,
    endDate: str,
    buildingName: Optional[str] = None,  # '0' for all buildings if needed
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingName": buildingName, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetHighDemandHoursByBuilding", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS BenchMarking/GetTemperatureHumidityImpact – Weather impact (temp/humidity) on daily energy for buildings at a site.")
async def ems_benchmarking_get_temperature_humidity_impact(
    startDate: str,
    endDate: str,
    site: Optional[str] = None,           # e.g., "Box Park"
    buildingName: Optional[str] = None,   # '0' for all buildings if needed
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"buildingName": buildingName, "startDate": startDate, "endDate": endDate, "site": site})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetTemperatureHumidityImpact", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS BenchMarking/GetHourlyEnergyProfiling – Hourly energy & intensity categorized by time-of-use.")
async def ems_benchmarking_get_hourly_energy_profiling(
    siteId: int,
    startDate: str,
    endDate: str,
    buildingName: Optional[str] = None,  # '0' for all if needed
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"startDate": startDate, "endDate": endDate, "siteId": siteId, "buildingName": buildingName})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetHourlyEnergyProfiling", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS BenchMarking/GetMonthlyEnergyProfiling – Monthly energy, intensity, trends per building.")
async def ems_benchmarking_get_monthly_energy_profiling(
    siteId: int,
    startDate: str,
    endDate: str,
    buildingName: Optional[str] = None,  # '0' for all if needed
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"buildingName": buildingName, "startDate": startDate, "endDate": endDate, "siteId": siteId})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetMonthlyEnergyProfiling", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS BenchMarking/GetWeekendWeekdayEnergyProfiling – Daily energy split by weekday vs weekend.")
async def ems_benchmarking_get_weekend_weekday_energy_profiling(
    siteId: int,
    startDate: str,
    endDate: str,
    buildingName: Optional[str] = None,  # '0' for all if needed
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"buildingName": buildingName, "startDate": startDate, "endDate": endDate, "siteId": siteId})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetWeekendWeekdayEnergyProfiling", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS BenchMarking/GetDailyEnergyConsumptionTrend – Daily energy trend per building/site.")
async def ems_benchmarking_get_daily_energy_consumption_trend(
    siteId: int,
    startDate: str,
    endDate: str,
    buildingName: Optional[str] = None,  # '0' for all if needed
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"buildingName": buildingName, "startDate": startDate, "endDate": endDate, "siteId": siteId})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetDailyEnergyConsumptionTrend", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS BenchMarking/GetIdleVsActivePowerUsageByBuilding – Baseline vs actual power usage with operational status.")
async def ems_benchmarking_get_idle_vs_active_power_usage_by_building(
    siteId: int,
    startDate: str,
    endDate: str,
    buildingName: Optional[str] = None,  # '0' for all if needed
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"buildingName": buildingName, "startDate": startDate, "endDate": endDate, "siteId": siteId})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetIdleVsActivePowerUsageByBuilding", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS BenchMarking/GetDailyEnergyProfiling – Daily energy with intensity & avg hourly for buildings.")
async def ems_benchmarking_get_daily_energy_profiling(
    siteId: int,
    startDate: str,
    endDate: str,
    buildingName: Optional[str] = None,  # '0' for all if needed
    *,
    bearer: Optional[str] = None
) -> Any:
    _need_token(bearer)
    params = _q({"buildingName": buildingName, "startDate": startDate, "endDate": endDate, "siteId": siteId})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetDailyEnergyProfiling", params=params)
        r.raise_for_status()
        return r.json()

# ---------- EnergyCentricMaintenance (fault / anomaly detection) ----------

@mcp.tool(description="EMS EnergyCentricMaintenance/GetEnergyConsumptionIrregularities – Daily consumption deviations vs baseline, anomaly flags.")
async def ems_ecm_get_energy_consumption_irregularities(
    startDate: str,
    endDate: str,
    buildingName: str,
    meterId: Optional[int] = None,   # use 0 for all meters if applicable
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "buildingName": buildingName,
        "meterId": meterId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetEnergyConsumptionIrregularities", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS EnergyCentricMaintenance/GetAnomalySummary – Summary of gaps/spikes/flatlines for a building (and meter).")
async def ems_ecm_get_anomaly_summary(
    buildingName: str,
    startDate: str,
    endDate: str,
    meterId: Optional[int] = None,   # use 0 for all meters if applicable
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "startDate": startDate,
        "endDate": endDate,
        "meterId": meterId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetAnomalySummary", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS EnergyCentricMaintenance/GetCurrentImbalanceDetection – Phase current (A/B/C) imbalance detection by month.")
async def ems_ecm_get_current_imbalance_detection(
    buildingName: str,
    startDate: str,
    endDate: str,
    meterId: Optional[int] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "startDate": startDate,
        "endDate": endDate,
        "meterId": meterId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetCurrentImbalanceDetection", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS EnergyCentricMaintenance/GetCurrentFlatlineDetection – Detects flatline current behavior across months.")
async def ems_ecm_get_current_flatline_detection(
    buildingName: str,
    startDate: str,
    endDate: str,
    meterId: Optional[int] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "startDate": startDate,
        "endDate": endDate,
        "meterId": meterId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetCurrentFlatlineDetection", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS EnergyCentricMaintenance/GetFrequencyDeviation – Frequency deviation monitoring (Hz) by month.")
async def ems_ecm_get_frequency_deviation(
    buildingName: str,
    startDate: str,
    endDate: str,
    meterId: Optional[int] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "startDate": startDate,
        "endDate": endDate,
        "meterId": meterId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetFrequencyDeviation", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS EnergyCentricMaintenance/GetHighApparentPowerorActivePower – Flags high apparent/active power conditions.")
async def ems_ecm_get_high_apparent_or_active_power(
    buildingName: str,
    startDate: str,
    endDate: str,
    meterId: Optional[int] = None,   # NOTE: API expects 'meterid' (lowercase d) in the querystring
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    # API parameter key is 'meterid' (not 'meterId'); map accordingly.
    params = _q({
        "buildingName": buildingName,
        "startDate": startDate,
        "endDate": endDate,
        "meterid": meterId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetHighApparentPowerorActivePower", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS EnergyCentricMaintenance/GetOvercurrentDetection – Overcurrent detection per month (phase currents).")
async def ems_ecm_get_overcurrent_detection(
    buildingName: str,
    startDate: str,
    endDate: str,
    meterId: Optional[int] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "startDate": startDate,
        "endDate": endDate,
        "meterId": meterId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetOvercurrentDetection", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS EnergyCentricMaintenance/GetOvervoltageandUndervoltageDetection – Voltage status (per phase) over time.")
async def ems_ecm_get_overvoltage_and_undervoltage_detection(
    buildingName: str,
    startDate: str,
    endDate: str,
    meterId: Optional[int] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "startDate": startDate,
        "endDate": endDate,
        "meterId": meterId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetOvervoltageandUndervoltageDetection", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS EnergyCentricMaintenance/GetPowerFactorAnomaliesSummary – Power factor anomaly summary per device.")
async def ems_ecm_get_power_factor_anomalies_summary(
    buildingName: str,
    startDate: str,
    endDate: str,
    meterId: Optional[int] = None,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "startDate": startDate,
        "endDate": endDate,
        "meterId": meterId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetPowerFactorAnomaliesSummary", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS EnergyCentricMaintenance/AbnormalPowerSpikesForecasting – Forecast abnormal power spikes for a building.")
async def ems_ecm_abnormal_power_spikes_forecasting(
    buildingName: str,
    startDate: str,
    endDate: str,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"buildingName": buildingName, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/AbnormalPowerSpikesForecasting", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS EnergyCentricMaintenance/LoadImbalanceForecasting – Forecast phase load imbalance.")
async def ems_ecm_load_imbalance_forecasting(
    building: str,                # NOTE: API key is 'building' (not buildingName)
    startDate: str,
    endDate: str,
    meterId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"building": building, "startDate": startDate, "endDate": endDate, "meterId": meterId})
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/LoadImbalanceForecasting", params=params)
        r.raise_for_status()
        return r.json()


# ---------- SustainabilityInsights (efficiency degradation) ----------

@mcp.tool(description="EMS SustainabilityInsights/GetEnergyEfficiencyDegradationData – Average & trend power factor over time.")
async def ems_sustainability_get_energy_efficiency_degradation_data(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingName: Optional[str] = None,
    meterId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "siteId": siteId,
        "buildingName": buildingName,
        "meterId": meterId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/SustainabilityInsights/GetEnergyEfficiencyDegradationData", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS Portfolio/GetWaterConsumption – Water consumption for date range; optional site/building filters.")
async def ems_portfolio_get_water_consumption(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingId: int = 0,
    isGallon: bool = False,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "siteId": siteId,
        "buildingId": buildingId,
        "isGallon": isGallon,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetWaterConsumption", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS Portfolio/GetWaterReadingHeatMap – Daily water reading heatmap; optional filters.")
async def ems_portfolio_get_water_reading_heatmap(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingTypeId: int = 0,
    buildingId: int = 0,
    meterId: int = 0,
    isGallon: bool = False,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "siteId": siteId,
        "buildingTypeId": buildingTypeId,
        "buildingId": buildingId,
        "meterId": meterId,
        "isGallon": isGallon,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetWaterReadingHeatMap", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS Portfolio/GetWaterCostSumByBuilding – Total water cost by building for a date range.")
async def ems_portfolio_get_water_cost_sum_by_building(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetWaterCostSumByBuilding", params=params)
        r.raise_for_status()
        return r.json()


# ---------- WaterUsageAnalysis ----------

@mcp.tool(description="EMS WaterUsageAnalysis/GetWaterStats – Portfolio/site/building water stats for date range.")
async def ems_water_usage_get_water_stats(
    siteId: int = 0,
    buildingId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/WaterUsageAnalysis/GetWaterStats", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS WaterUsageAnalysis/GetWaterCostConsumptionData – Water cost by site/building.")
async def ems_water_usage_get_water_cost_consumption_data(
    siteId: int = 0,
    buildingId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/WaterUsageAnalysis/GetWaterCostConsumptionData", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS WaterUsageAnalysis/GetWaterConsumptionCostByBuilding – Water cost by building.")
async def ems_water_usage_get_water_consumption_cost_by_building(
    siteId: int = 0,
    buildingId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/WaterUsageAnalysis/GetWaterConsumptionCostByBuilding", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS WaterUsageAnalysis/GetWaterConsumptionStats – Summary stats and health for water meters.")
async def ems_water_usage_get_water_consumption_stats(
    siteId: int = 0,
    buildingId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/WaterUsageAnalysis/GetWaterConsumptionStats", params=params)
        r.raise_for_status()
        return r.json()


# ---------- UsageIntensity (Water) ----------

@mcp.tool(description="EMS UsageIntensity/GetWaterUsageIntensity – Water usage intensity (WUI) per building/time period.")
async def ems_usageintensity_get_water_usage_intensity(
    siteId: int = 0,
    buildingId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/UsageIntensity/GetWaterUsageIntensity", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS UsageIntensity/GetBuildingWiseWaterUsageIntensityPercentage – WUI and percentage share by building.")
async def ems_usageintensity_get_building_wise_wui_percentage(
    siteId: int = 0,
    buildingId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/UsageIntensity/GetBuildingWiseWaterUsageIntensityPercentage", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS UsageIntensity/GetBuildingandYearWiseWui – Year-wise WUI by building and month.")
async def ems_usageintensity_get_building_and_year_wise_wui(
    siteId: int = 0,
    buildingId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/UsageIntensity/GetBuildingandYearWiseWui", params=params)
        r.raise_for_status()
        return r.json()


# ---------- UnitConversion (Water) ----------

@mcp.tool(description="EMS UnitConversion/GetWaterConsumptionTrendsOverTime – Monthly water trends (m³, liters, gallons, cost).")
async def ems_unitconversion_get_water_consumption_trends_over_time(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"startDate": startDate, "endDate": endDate, "siteId": siteId, "buildingId": buildingId})
    async with _client_ems(bearer) as c:
        r = await c.get("/UnitConversion/GetWaterConsumptionTrendsOverTime", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS UnitConversion/WaterConsumptionVsEstimatedCost – Compare water usage vs estimated cost.")
async def ems_unitconversion_water_consumption_vs_estimated_cost(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"startDate": startDate, "endDate": endDate, "siteId": siteId, "buildingId": buildingId})
    async with _client_ems(bearer) as c:
        r = await c.get("/UnitConversion/WaterConsumptionVsEstimatedCost", params=params)
        r.raise_for_status()
        return r.json()


# ---------- BenchMarking (Water) ----------

@mcp.tool(description="EMS BenchMarking/GetSiteLevelWaterUsageIntensity – Site-level water usage intensity by month.")
async def ems_benchmarking_get_site_level_water_usage_intensity(
    siteId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelWaterUsageIntensity", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS BenchMarking/GetSiteLevelWaterCostIntensity – Site-level water cost intensity by month.")
async def ems_benchmarking_get_site_level_water_cost_intensity(
    siteId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelWaterCostIntensity", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS BenchMarking/GetSiteLevelPeakVsOffPeakWaterUsage – Peak vs off-peak site water usage by day.")
async def ems_benchmarking_get_site_level_peak_vs_offpeak_water_usage(
    siteId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelPeakVsOffPeakWaterUsage", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS BenchMarking/GetSiteLevelWaterLeakageAndFlatlineDetection – Leakage/flatline detection by site/day.")
async def ems_benchmarking_get_site_level_water_leakage_and_flatline_detection(
    siteId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelWaterLeakageAndFlatlineDetection", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS BenchMarking/GetSiteLevelPerCapitaWaterConsumption – Per-capita site water consumption by day.")
async def ems_benchmarking_get_site_level_per_capita_water_consumption(
    siteId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelPerCapitaWaterConsumption", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS BenchMarking/GetSiteLevelWaterBenchmarkingAgainstIndustryStandards – Benchmark vs industry per-capita.")
async def ems_benchmarking_get_site_level_water_benchmarking_against_industry_standards(
    siteId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelWaterBenchmarkingAgainstIndustryStandards", params=params)
        r.raise_for_status()
        return r.json()


# ---------- TimeProfiling (Water) ----------

@mcp.tool(description="EMS TimeProfiling/GetSiteLevelAnnualWaterConsumption – Annual site water consumption and intensities.")
async def ems_timeprofiling_get_site_level_annual_water_consumption(
    siteId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"startDate": startDate, "endDate": endDate, "siteId": siteId})
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetSiteLevelAnnualWaterConsumption", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS TimeProfiling/GetSiteLevelDailyWaterConsumption – Daily site water consumption and intensities.")
async def ems_timeprofiling_get_site_level_daily_water_consumption(
    siteId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetSiteLevelDailyWaterConsumption", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS TimeProfiling/GetSiteLevelMonthlyWaterConsumption – Monthly site water consumption and intensities.")
async def ems_timeprofiling_get_site_level_monthly_water_consumption(
    siteId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetSiteLevelMonthlyWaterConsumption", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS TimeProfiling/GetSiteLevelPredictiveWaterProfiling – Predictive water usage by site.")
async def ems_timeprofiling_get_site_level_predictive_water_profiling(
    siteId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetSiteLevelPredictiveWaterProfiling", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS TimeProfiling/GetWaterMeterHighDemandHoursProfiling – Hourly water demand status by meter/building.")
async def ems_timeprofiling_get_water_meter_high_demand_hours_profiling(
    siteId: int = 0,
    buildingId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetWaterMeterHighDemandHoursProfiling", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS TimeProfiling/GetWaterMeterWeekdayWeekendProfiling – Weekday vs weekend water usage by building.")
async def ems_timeprofiling_get_water_meter_weekday_weekend_profiling(
    siteId: int = 0,
    buildingId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetWaterMeterWeekdayWeekendProfiling", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS SustainabilityInsights/GetMetersByBuildings – List energy meters for a building.")
async def ems_sustainability_get_meters_by_buildings(
    buildingName: str,
    applicationName: str = "EnergyMeter",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"buildingName": buildingName, "applicationName": applicationName})
    async with _client_ems(bearer) as c:
        r = await c.get("/SustainabilityInsights/GetMetersByBuildings", params=params)
        r.raise_for_status()
        return r.json()


# ---------- EnergyCentricMaintenance – Water Fault Detection ----------

@mcp.tool(description="EMS EnergyCentricMaintenance/GetFlatlineDetectionWaterMeter – Detect flatline states in water flow readings.")
async def ems_ecm_get_flatline_detection_water_meter(
    buildingName: Optional[str] = None,
    meterId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "meterId": meterId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetFlatlineDetectionWaterMeter", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS EnergyCentricMaintenance/GetFlowRateIrregularities – Detect abnormal water flow vs rolling baseline.")
async def ems_ecm_get_flow_rate_irregularities(
    buildingName: Optional[str] = None,
    meterId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "meterId": meterId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetFlowRateIrregularities", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS EnergyCentricMaintenance/GetPressureImbalanceDetection – Detect pressure imbalance events.")
async def ems_ecm_get_pressure_imbalance_detection(
    buildingName: Optional[str] = None,
    meterId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "meterId": meterId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetPressureImbalanceDetection", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS EnergyCentricMaintenance/LeakDetectionAndAnomalyDetection – Daily leak & anomaly flags for buildings.")
async def ems_ecm_leak_detection_and_anomaly_detection(
    startDate: str,
    endDate: str,
    buildingName: Optional[str] = None,
    meterId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "buildingName": buildingName,
        "meterId": meterId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/LeakDetectionAndAnomalyDetection", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS EnergyCentricMaintenance/GetWaterEfficiencyRatingsAndCostOptimization – Ratings & recommendations.")
async def ems_ecm_get_water_efficiency_ratings_and_cost_optimization(
    startDate: str,
    endDate: str,
    buildingName: Optional[str] = None,
    meterId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "buildingName": buildingName,
        "meterId": meterId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetWaterEfficiencyRatingsAndCostOptimization", params=params)
        r.raise_for_status()
        return r.json()

@mcp.tool(description="EMS Portfolio/GetChilledWaterConsumption – Chilled water consumption by building for date range.")
async def ems_portfolio_get_chilled_water_consumption(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "siteId": siteId,
        "buildingId": buildingId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetChilledWaterConsumption", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS Portfolio/GetBtuTotalHeatMap – Daily BTU (thermal energy) heatmap.")
async def ems_portfolio_get_btu_total_heatmap(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingTypeId: int = 0,
    buildingId: int = 0,
    meterId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "siteId": siteId,
        "buildingTypeId": buildingTypeId,
        "buildingId": buildingId,
        "meterId": meterId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetBtuTotalHeatMap", params=params)
        r.raise_for_status()
        return r.json()


# ---------- ChilledWater (stats) ----------

@mcp.tool(description="EMS ChilledWater/GetChilledWaterStats – Summary stats for chilled water systems.")
async def ems_chilledwater_get_chilled_water_stats(
    siteId: int = 0,
    buildingId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    """
    Returns chilled water KPIs (supply/return temperatures, flow rates, BTU totals) for the given scope.
    """
    _need_token(bearer)
    params = _q({"siteId": siteId, "buildingId": buildingId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/ChilledWater/GetChilledWaterStats", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS ChilledWater/GetTotalChilledWaterCostByBuildingSite – Total chilled water cost by site with building breakdown.")
async def ems_chilledwater_get_total_chilled_water_cost_by_building_site(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "siteId": siteId,
        "buildingId": buildingId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/ChilledWater/GetTotalChilledWaterCostByBuildingSite", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS Portfolio/GetChilledWaterCostSumByBuilding – Portfolio-wide chilled water cost totals per building.")
async def ems_portfolio_get_chilled_water_cost_sum_by_building(
    startDate: str,
    endDate: str,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/Portfolio/GetChilledWaterCostSumByBuilding", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS ChilledWater/GetChilledWaterCostConsumption – Cost per building for the date range.")
async def ems_chilledwater_get_chilled_water_cost_consumption(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "buildingId": buildingId,
        "siteId": siteId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/ChilledWater/GetChilledWaterCostConsumption", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS ChilledWater/GetChilledWaterConsumptionStats – Summary stats (temps, flow, energy).")
async def ems_chilledwater_get_chilled_water_consumption_stats(
    siteId: int = 0,
    buildingId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "siteId": siteId,
        "buildingId": buildingId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/ChilledWater/GetChilledWaterConsumptionStats", params=params)
        r.raise_for_status()
        return r.json()


# ---------- UsageIntensity – CUI/BTU EUI ----------

@mcp.tool(description="EMS UsageIntensity/GetChilledWaterEnergyIntensity – BTU EUI per building and period.")
async def ems_usage_get_chilled_water_energy_intensity(
    siteId: int = 0,
    buildingId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "siteId": siteId,
        "buildingId": buildingId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/UsageIntensity/GetChilledWaterEnergyIntensity", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS UsageIntensity/GetBuildingWiseChilledWaterUsageIntensityPercentage – EUI % share per building.")
async def ems_usage_get_building_wise_chilled_water_usage_intensity_percentage(
    siteId: int = 0,
    buildingId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "siteId": siteId,
        "buildingId": buildingId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/UsageIntensity/GetBuildingWiseChilledWaterUsageIntensityPercentage", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS UsageIntensity/GetBuildingandYearWiseCUI – Year-wise chilled water intensity by building and month.")
async def ems_usage_get_building_and_year_wise_cui(
    siteId: int = 0,
    buildingId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "siteId": siteId,
        "buildingId": buildingId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/UsageIntensity/GetBuildingandYearWiseCUI", params=params)
        r.raise_for_status()
        return r.json()


# ---------- UnitConversion – Thermal energy over time ----------

@mcp.tool(description="EMS UnitConversion/GetThermalEnergyConversionOverTime – BTU/kWh/J & cost over time.")
async def ems_unit_get_thermal_energy_conversion_over_time(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "siteId": siteId,
        "buildingId": buildingId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/UnitConversion/GetThermalEnergyConversionOverTime", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS UnitConversion/ThermalEnergyVsEstimatedCost – Compare thermal energy vs estimated cost.")
async def ems_unit_thermal_energy_vs_estimated_cost(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "siteId": siteId,
        "buildingId": buildingId,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/UnitConversion/ThermalEnergyVsEstimatedCost", params=params)
        r.raise_for_status()
        return r.json()


# ---------- BenchMarking – BTU benchmarks ----------

@mcp.tool(description="EMS BenchMarking/GetSiteLevelBtuBenchmarkData – Actual vs best-practice thermal consumption.")
async def ems_benchmarking_get_site_level_btu_benchmark_data(
    siteId: int = 0,
    startDate: str = "",
    endDate: str = "",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelBtuBenchmarkData", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS BenchMarking/GetSiteLevelBtuMeterCarbonDeviationAnalysis – Actual vs best-practice carbon for BTU meters.")
async def ems_benchmarking_get_site_level_btu_meter_carbon_deviation_analysis(
    startDate: str,
    endDate: str,
    siteIds: str = "0",  # CSV supported, e.g. "335,336"
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteIds": siteIds, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelBtuMeterCarbonDeviationAnalysis", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS BenchMarking/GetSiteLevelBtuMeterCarbonEmissionPerformance – Carbon totals, intensity & category for BTU meters.")
async def ems_benchmarking_get_site_level_btu_meter_carbon_emission_performance(
    startDate: str,
    endDate: str,
    siteIds: str = "0",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteIds": siteIds, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelBtuMeterCarbonEmissionPerformance", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS BenchMarking/GetSiteLevelBtuMeterCrossSectionalBenchmarkingSummary – Cooling/carbon intensity & performance summary.")
async def ems_benchmarking_get_site_level_btu_meter_cross_sectional_benchmarking_summary(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteId": siteId, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelBtuMeterCrossSectionalBenchmarkingSummary", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS BenchMarking/GetSiteLevelBtuMeterEnergyDeviationAnalysis – Actual vs expected BTU, deviation & status.")
async def ems_benchmarking_get_site_level_btu_meter_energy_deviation_analysis(
    startDate: str,
    endDate: str,
    siteIds: str = "0",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteIds": siteIds, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelBtuMeterEnergyDeviationAnalysis", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS BenchMarking/GetSiteLevelCoolingIntensityComparison – Cooling intensity (RT/m²) by site with efficiency category.")
async def ems_benchmarking_get_site_level_cooling_intensity_comparison(
    startDate: str,
    endDate: str,
    siteIds: str = "0",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"siteIds": siteIds, "startDate": startDate, "endDate": endDate})
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetSiteLevelCoolingIntensityComparison", params=params)
        r.raise_for_status()
        return r.json()


# ---------- TimeProfiling – Annual / Monthly / Weekly BTU ----------

@mcp.tool(description="EMS TimeProfiling/GetSiteLevelAnnualBtuAnalysis – Yearly BTU analysis, intensity & ratings by site.")
async def ems_timeprofiling_get_site_level_annual_btu_analysis(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"startDate": startDate, "endDate": endDate, "siteId": siteId})
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetSiteLevelAnnualBtuAnalysis", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS TimeProfiling/GetSiteLevelMonthlyBTUAnalysis – Monthly BTU analysis, intensity & trends by site.")
async def ems_timeprofiling_get_site_level_monthly_btu_analysis(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"startDate": startDate, "endDate": endDate, "siteId": siteId})
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetSiteLevelMonthlyBTUAnalysis", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS TimeProfiling/GetSiteLevelWeeklyBTUAnalysis – Weekly BTU analysis, intensity & efficiency by site.")
async def ems_timeprofiling_get_site_level_weekly_btu_analysis(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({"startDate": startDate, "endDate": endDate, "siteId": siteId})
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetSiteLevelWeeklyBTUAnalysis", params=params)
        r.raise_for_status()
        return r.json()


# ---------- Profiling – High demand hours & weekday/weekend (BTU) ----------

@mcp.tool(description="EMS BenchMarking/GetHighDemandHoursByBuildingBTU – Hourly BTU and demand status per building.")
async def ems_benchmarking_get_high_demand_hours_by_building_btu(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingName: str = "0",  # "0" = all buildings, else exact name
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "siteId": siteId,
        "buildingName": buildingName,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/BenchMarking/GetHighDemandHoursByBuildingBTU", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS TimeProfiling/GetWeekendWeekdayBTUProfiling – Daily BTU by building with weekday/weekend flag.")
async def ems_timeprofiling_get_weekend_weekday_btu_profiling(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingName: str = "0",
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "startDate": startDate,
        "endDate": endDate,
        "siteId": siteId,
        "buildingName": buildingName,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetWeekendWeekdayBTUProfiling", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS TimeProfiling/GetDailyBTUConsumptionTrend – Daily BTU (kWh) trend by site/building with weekday/weekend flag.")
async def ems_timeprofiling_get_daily_btu_consumption_trend(
    startDate: str,
    endDate: str,
    siteId: int = 0,
    buildingName: str = "0",  # '0' = all buildings; else exact name
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "siteId": siteId,
        "buildingName": buildingName,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/TimeProfiling/GetDailyBTUConsumptionTrend", params=params)
        r.raise_for_status()
        return r.json()


# ---------- EnergyCentricMaintenance – BTU Meter diagnostics ----------

@mcp.tool(description="EMS ECM/GetEnergyConsumptionIrregularitiesBTUMeter – Detects BTU daily-consumption anomalies vs rolling avg/stddev.")
async def ems_ecm_get_energy_consumption_irregularities_btu_meter(
    startDate: str,
    endDate: str,
    buildingName: str = "0",
    meterId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "meterId": meterId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetEnergyConsumptionIrregularitiesBTUMeter", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS ECM/GetEnergyImbalanceBTUMeter – Energy imbalance detection using rolling statistics.")
async def ems_ecm_get_energy_imbalance_btu_meter(
    startDate: str,
    endDate: str,
    buildingName: str = "0",
    meterId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "meterId": meterId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetEnergyImbalanceBTUMeter", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS ECM/GetFlowImbalanceDetectionBTUMeter – Flow rate imbalance detection for BTU meters.")
async def ems_ecm_get_flow_imbalance_detection_btu_meter(
    startDate: str,
    endDate: str,
    buildingName: str = "0",
    meterId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "meterId": meterId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetFlowImbalanceDetectionBTUMeter", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS ECM/GetSensorMalfunctionDetectionBTUMeter – Flags sensor faults (e.g., negative flow, temp spikes).")
async def ems_ecm_get_sensor_malfunction_detection_btu_meter(
    startDate: str,
    endDate: str,
    buildingName: str = "0",
    meterId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "meterId": meterId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetSensorMalfunctionDetectionBTUMeter", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS ECM/GetThermalLossDetection – Thermal loss diagnostics by month for chilled water (uses 'building' param).")
async def ems_ecm_get_thermal_loss_detection(
    startDate: str,
    endDate: str,
    building: str = "0",  # NOTE: endpoint expects 'building', not 'buildingName'
    meterId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "building": building,
        "meterId": meterId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetThermalLossDetection", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS ECM/GetPredictiveMaintenanceScheduling – Daily maintenance status from flow/temp/thermal-loss signals.")
async def ems_ecm_get_predictive_maintenance_scheduling(
    startDate: str,
    endDate: str,
    buildingName: str = "0",
    meterId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "meterId": meterId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetPredictiveMaintenanceScheduling", params=params)
        r.raise_for_status()
        return r.json()


@mcp.tool(description="EMS ECM/GetEnergyTrendsMonitoring – Daily energy, rolling stats & forecast for BTU meters.")
async def ems_ecm_get_energy_trends_monitoring(
    startDate: str,
    endDate: str,
    buildingName: str = "0",
    meterId: int = 0,
    *,
    bearer: Optional[str] = None,
) -> Any:
    _need_token(bearer)
    params = _q({
        "buildingName": buildingName,
        "meterId": meterId,
        "startDate": startDate,
        "endDate": endDate,
    })
    async with _client_ems(bearer) as c:
        r = await c.get("/EnergyCentricMaintenance/GetEnergyTrendsMonitoring", params=params)
        r.raise_for_status()
        return r.json()
# ---------- ASGI app (Streamable HTTP at /mcp) ----------
# from starlette.applications import Starlette
# from starlette.routing import Mount
# NOTE: streamable_http_app already exposes the protocol app; we mount it at /mcp from the server entrypoint.
mcp_asgi = mcp.streamable_http_app()

app = mcp_asgi  # served at root; gateway points to /mcp (handled internally by FastMCP)
# app = Starlette(routes=[
#     Mount("/mcp", app=mcp_asgi),  # exact path; no trailing slash
# ])
if __name__ == "__main__":
    import uvicorn
    # Keep path consistent with gateway MCP_URL=http://localhost:8001/mcp
    uvicorn.run("server:app", host="0.0.0.0", port=8002, reload=True)
