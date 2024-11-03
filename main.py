import asyncio
import random
import time
from datetime import datetime
from typing import Union, List, Dict, AsyncGenerator, Any, Annotated

from fastapi import FastAPI, Depends
from fastapi.responses import JSONResponse
import uvicorn
from sqlalchemy import text, CursorResult, Result, select, MetaData, create_engine, func, BigInteger, String, Integer, \
    Table
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession, AsyncAttrs
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, class_mapper
from sqlalchemy.sql.base import ReadOnlyColumnCollection

app = FastAPI()
async_engine = create_async_engine('sqlite+aiosqlite:///sqlite.db', echo=False)
async_session = async_sessionmaker(async_engine, expire_on_commit=False)


async def get_db() -> AsyncGenerator[AsyncSession, Any]:
    async with async_session() as session, session.begin():
        yield session


SessionDep = Annotated[AsyncSession, Depends(get_db)]


class Base(AsyncAttrs, DeclarativeBase):
    created_at: Mapped[datetime] = mapped_column(server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(server_default=func.now(), onupdate=func.now())


class Tasks(Base):
    __tablename__ = 'tasks'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    task_data: Mapped[str] = mapped_column(String, nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=True)

    def __repr__(self):
        return f'<Tasks(id={self.id}, task_data={self.task_data}, status={self.status})>, created_at={self.created_at}, updated_at={self.updated_at})'

    def json(self):
        columns = [c.key for c in class_mapper(self.__class__).columns]
        return {c: getattr(self, c) for c in columns}


async def create_tables():
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


# asyncio.run(create_tables())


class TaskBroker:
    meta: MetaData

    def __init__(self, session):
        self.session: AsyncSession = session
        self.run()
        self.table_tasks = self.meta.tables['tasks']

    # async def run(self):
    #     meta = MetaData()
    #     async with async_engine.begin() as conn:
    #         await conn.run_sync(meta.reflect)
    #     self.meta = meta
    def run(self):
        int_engine = create_engine(url='sqlite:///sqlite.db')
        int_meta = MetaData()
        int_meta.reflect(bind=int_engine)
        self.meta: MetaData = int_meta

    @staticmethod
    def to_dict(model: Union[CursorResult, Result], one: bool = False) -> Union[List[Dict], Dict, bool]:
        columns = model.keys()
        model_dict = []

        if not one:
            for i_m in model.fetchall():
                model_dict.append(dict(zip(columns, i_m)))
            return model_dict
        try:
            return dict(zip(columns, model.first()))
        except TypeError:
            return False

    async def add_task(self, task_data: str):
        await self.session.execute(text(f"INSERT INTO tasks (task_data, status) VALUES ('{task_data}', 'pending')"))
        await self.session.commit()
        return

    async def get_task(self):
        # sql = (
        #     select(self.table_tasks)
        #     .with_only_columns(
        #         self.table_tasks.c.task_data,
        #         self.table_tasks.c.status,
        #     )
        #     .where(self.table_tasks.c.status == 'pending')
        #     .limit(1)
        # )
        # data = await self.session.execute(sql)
        # return self.to_dict(data, one=True)
        data = await self.session.scalar(select(Tasks))
        # num = random.randint(1, 10)
        # print(f'{num=}')
        # time.sleep(num)
        print(data)
        # result = await self.session.execute(select(Tasks))
        # notes = result.scalars().all()
        # print(notes)
        # print(notes)
        # return self.to_dict(data, one=True)
        return data.json()

    async def delete_task(self, task_id):
        await self.session.execute(text(f"DELETE FROM tasks WHERE id = {task_id}"))
        await self.session.commit()
        return


@app.get("/task")
async def get_task(
        session: SessionDep
):
    broker = TaskBroker(session)
    data = await broker.get_task()
    return data


@app.post("/task")
async def add_task(task_data: str, session: SessionDep):
    broker = TaskBroker(session)
    await broker.add_task(task_data)
    return JSONResponse({"task_id": '1', "task_data": '2'})


@app.delete("/task/{task_id}")
async def delete_task(task_id: int, session: SessionDep):
    broker = TaskBroker(session)
    await broker.delete_task(task_id)
    return JSONResponse({"message": f"Task {task_id} deleted."})
