# C:\Users\User\Downloads\buildot-mcp-stack\mcp_server\server.py
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
