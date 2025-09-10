from __future__ import annotations

import sys
from pathlib import Path

import click

from .convert import convert_path


@click.group(name="helios")
def app() -> None:
    """
    Helios CLI

    한국어 주석: Helios는 Oracle SQL(.sql 파일)을 Spark SQL로 변환합니다.
    정확성 우선 정책을 따르며, 안전하지 않은 경우 실패로 표시합니다.
    """


@app.command(name="convert")
@click.argument("path", type=click.Path(exists=True, dir_okay=False, file_okay=True, path_type=str))
@click.option("--use-llm/--no-llm", default=True, help="LLM 보조 변환 사용 여부 (기본 사용)")
@click.option(
    "--provider",
    type=click.Choice(["hive", "delta", "iceberg"], case_sensitive=False),
    default="hive",
    show_default=True,
    help="타깃 테이블 포맷에 따른 변환 전략",
)
def convert_cmd(path: str, use_llm: bool, provider: str) -> None:
    """
    Convert a single .sql file and write <name>_helios.sql next to it.

    한국어 주석: 입력은 단일 .sql 파일만 지원합니다. 
    결과는 동일 위치에 _helios 접미사로 저장됩니다.
    """
    try:
        summary = convert_path(path, use_llm=use_llm, provider=provider)
        click.echo(summary)
    except Exception as exc:  # noqa: BLE001 - surface errors to user
        click.echo(f"Conversion failed: {exc}", err=True)
        sys.exit(1)


