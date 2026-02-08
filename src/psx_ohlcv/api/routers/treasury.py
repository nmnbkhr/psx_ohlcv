"""Treasury API endpoints — T-Bill, PIB, GIS auctions and yield data."""

from fastapi import APIRouter, Query
from typing import Optional

from ...db import (
    connect,
    init_schema,
    get_tbill_auctions,
    get_pib_auctions,
    get_gis_auctions,
    get_latest_tbill_yields,
    get_latest_pib_yields,
    get_yield_trend,
    get_pkrv_curve,
    get_pkrv_history,
    compare_curves,
)

router = APIRouter()


@router.get("/tbills")
def list_tbill_auctions(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    tenor: Optional[str] = Query(None, description="Tenor filter (e.g. '3M', '6M', '12M')"),
):
    """Get T-Bill auction history with optional filters."""
    con = connect()
    init_schema(con)
    df = get_tbill_auctions(con, start_date=start_date, end_date=end_date, tenor=tenor)
    return {"count": len(df), "auctions": df.to_dict(orient="records")}


@router.get("/tbills/latest")
def latest_tbill_yields():
    """Get latest cutoff yields for all T-Bill tenors."""
    con = connect()
    init_schema(con)
    yields = get_latest_tbill_yields(con)
    return {"count": len(yields), "yields": yields}


@router.get("/tbills/trend/{tenor}")
def tbill_yield_trend(
    tenor: str,
    n_auctions: int = Query(20, description="Number of recent auctions"),
):
    """Get yield trend for a specific T-Bill tenor."""
    con = connect()
    init_schema(con)
    df = get_yield_trend(con, tenor=tenor, n_auctions=n_auctions)
    return {"tenor": tenor, "count": len(df), "trend": df.to_dict(orient="records")}


@router.get("/pibs")
def list_pib_auctions(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
):
    """Get PIB auction history."""
    con = connect()
    init_schema(con)
    df = get_pib_auctions(con, start_date=start_date, end_date=end_date)
    return {"count": len(df), "auctions": df.to_dict(orient="records")}


@router.get("/pibs/latest")
def latest_pib_yields():
    """Get latest cutoff yields for all PIB tenors."""
    con = connect()
    init_schema(con)
    yields = get_latest_pib_yields(con)
    return {"count": len(yields), "yields": yields}


@router.get("/gis")
def list_gis_auctions(
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
):
    """Get GIS (Government Ijarah Sukuk) auction history."""
    con = connect()
    init_schema(con)
    df = get_gis_auctions(con, start_date=start_date, end_date=end_date)
    return {"count": len(df), "auctions": df.to_dict(orient="records")}


@router.get("/yields")
def latest_yields():
    """Get latest yields across all treasury instruments."""
    con = connect()
    init_schema(con)
    tbill = get_latest_tbill_yields(con)
    pib = get_latest_pib_yields(con)
    return {"tbill": tbill, "pib": pib}


@router.get("/curve/pkrv")
def pkrv_yield_curve(
    date: Optional[str] = Query(None, description="Date (YYYY-MM-DD), latest if omitted"),
):
    """Get PKRV yield curve for a given date."""
    con = connect()
    init_schema(con)
    df = get_pkrv_curve(con, date=date)
    records = df.to_dict(orient="records") if not df.empty else []
    curve_date = records[0]["date"] if records else date
    return {"date": curve_date, "count": len(records), "curve": records}


@router.get("/curve/pkrv/history/{tenor_months}")
def pkrv_tenor_history(
    tenor_months: int,
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Get PKRV history for a specific tenor."""
    con = connect()
    init_schema(con)
    df = get_pkrv_history(con, tenor_months=tenor_months, start_date=start_date, end_date=end_date)
    return {"tenor_months": tenor_months, "count": len(df), "history": df.to_dict(orient="records")}


@router.get("/curve/pkrv/compare")
def pkrv_compare(
    date1: str = Query(..., description="First date (YYYY-MM-DD)"),
    date2: str = Query(..., description="Second date (YYYY-MM-DD)"),
):
    """Compare two PKRV yield curves side-by-side."""
    con = connect()
    init_schema(con)
    df = compare_curves(con, date1, date2)
    return {"date1": date1, "date2": date2, "comparison": df.to_dict(orient="records")}
