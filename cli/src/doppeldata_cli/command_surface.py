"""Every command the `doppeldata` tree holds, read from the tree itself, as `capabilities show` lists them."""

import re
from typing import Any

import typer
from typer._click import types as click_types  # Typer vendors click, and exports none of its types


def command_surface(ctx: typer.Context) -> list[dict[str, Any]]:
    """Every command under the root of `ctx`, in the order its help lists them."""
    commands: list[dict[str, Any]] = []
    walk(ctx, ctx.find_root().command, (), commands)
    return commands


def walk(ctx: typer.Context, group: Any, path: tuple[str, ...], commands: list[dict[str, Any]]) -> None:
    for name in group.list_commands(ctx):
        command = group.get_command(ctx, name)
        if hasattr(command, 'list_commands'):
            walk(ctx, command, (*path, name), commands)
        else:
            commands.append(described((*path, name), command))


def described(path: tuple[str, ...], command: Any) -> dict[str, Any]:
    return {
        'path': list(path),
        'summary': first_paragraph(command.help),
        'arguments': [
            {'name': parameter.metavar or parameter.name.upper(), 'required': parameter.required, 'help': first_paragraph(parameter.help)}
            for parameter in command.params
            if parameter.param_type_name == 'argument'
        ],
        'flags': [flag(parameter) for parameter in command.params if parameter.param_type_name == 'option'],
    }


def flag_names(option: Any) -> list[str]:
    """An option's names, the one it is listed and refused by first: its first long name, else its first short one."""
    return sorted(option.opts, key=lambda name: (not name.startswith('--'), name))


def flag(option: Any) -> dict[str, Any]:
    names = flag_names(option)
    choices = getattr(option.type, 'choices', None)
    return {
        'name': names[0],
        'aliases': names[1:],
        'type': flag_type(option),
        'choices': list(choices) if choices is not None else None,
        'multiple': option.multiple,
        'required': option.required,
        'default': plain(option.default),
        'help': first_paragraph(option.help),
    }


def flag_type(option: Any) -> str:
    """A flag's type, read from its class, because the names click gives its types differ between releases.

    A range subclasses its number type. A flag whose value a function parses takes text.
    """
    option_type = option.type
    if option.is_flag or isinstance(option_type, click_types.BoolParamType):
        return 'boolean'
    if isinstance(option_type, click_types.FuncParamType):
        return 'string'
    if isinstance(option_type, click_types.IntParamType):
        return 'integer'
    if isinstance(option_type, click_types.FloatParamType):
        return 'number'
    if getattr(option_type, 'choices', None) is not None:
        return 'choice'
    # A path type names itself for what it accepts: a file, a directory, or either.
    if isinstance(option_type, click_types.File) or option_type.name in ('path', 'file', 'directory'):
        return 'path'
    return 'string'


def plain(value: Any) -> Any:
    if value is None or isinstance(value, bool | int | float | str):
        return value
    if isinstance(value, list | tuple):
        return [plain(item) for item in value]
    return str(value)


def first_paragraph(text: str | None) -> str | None:
    if not text:
        return None
    return re.sub(r'\s+', ' ', text.strip().split('\n\n')[0])
