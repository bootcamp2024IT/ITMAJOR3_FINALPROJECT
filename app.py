from fastapi import FastAPI, HTTPException, Depends, Query
from pydantic import BaseModel
from typing import List, Optional
import mysql.connector
from mysql.connector import MySQLConnection
from datetime import datetime

app = FastAPI()


def get_db():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="",
        database="Flow_T"
    )
    try:
        yield conn
    finally:
        conn.close()


class TaskBase(BaseModel):
    title: str
    description: Optional[str] = None
    completed: Optional[bool] = False
    priority: Optional[int] = None

class TaskCreate(TaskBase):
    pass

class TaskUpdate(BaseModel):
    title: Optional[str] = None
    description: Optional[str] = None
    completed: Optional[bool] = None
    priority: Optional[int] = None

class Task(TaskBase):
    id: int
    created_at: datetime

    class Config:
        orm_mode = True


def initialize_database():
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password=""
    )
    cursor = conn.cursor()
    
    
    cursor.execute("CREATE DATABASE IF NOT EXISTS Flow_T")
    conn.database = "Flow_T"
    
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS tasks (
        id INT AUTO_INCREMENT PRIMARY KEY,
        title VARCHAR(255) NOT NULL,
        description TEXT,
        completed BOOLEAN DEFAULT FALSE,
        priority INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """)
    
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS task_comments (
        id INT AUTO_INCREMENT PRIMARY KEY,
        task_id INT,
        comment TEXT,
        FOREIGN KEY (task_id) REFERENCES tasks(id)
    )
    """)
    
    cursor.close()
    conn.close()

initialize_database()



@app.post("/tasks/", response_model=Task)
def create_task(task: TaskCreate, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute(
        "INSERT INTO tasks (title, description, completed, priority) VALUES (%s, %s, %s, %s)",
        (task.title, task.description, task.completed, task.priority)
    )
    db.commit()
    task_id = cursor.lastrowid
    cursor.execute("SELECT * FROM tasks WHERE id = %s", (task_id,))
    new_task = cursor.fetchone()
    cursor.close()
    return Task(**new_task)

@app.get("/tasks/", response_model=List[Task])
def read_tasks(skip: int = Query(0, ge=0), limit: int = Query(10, le=100), db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM tasks LIMIT %s OFFSET %s", (limit, skip))
    tasks = cursor.fetchall()
    cursor.close()
    return [Task(**task) for task in tasks]

@app.get("/tasks/{task_id}", response_model=Task)
def read_task(task_id: int, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM tasks WHERE id = %s", (task_id,))
    task = cursor.fetchone()
    cursor.close()
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return Task(**task)

@app.put("/tasks/{task_id}", response_model=Task)
def update_task(task_id: int, task: TaskUpdate, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    updates = []
    values = []

    if task.title is not None:
        updates.append("title = %s")
        values.append(task.title)
    if task.description is not None:
        updates.append("description = %s")
        values.append(task.description)
    if task.completed is not None:
        updates.append("completed = %s")
        values.append(task.completed)
    if task.priority is not None:
        updates.append("priority = %s")
        values.append(task.priority)

    if not updates:
        cursor.close()
        raise HTTPException(status_code=400, detail="No update parameters provided")

    values.append(task_id)
    update_str = ", ".join(updates)
    cursor.execute(f"UPDATE tasks SET {update_str} WHERE id = %s", tuple(values))
    db.commit()

    cursor.execute("SELECT * FROM tasks WHERE id = %s", (task_id,))
    updated_task = cursor.fetchone()
    cursor.close()

    if updated_task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    
    return Task(**updated_task)

@app.delete("/tasks/{task_id}", response_model=Task)
def delete_task(task_id: int, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM tasks WHERE id = %s", (task_id,))
    task = cursor.fetchone()

    if task is None:
        cursor.close()
        raise HTTPException(status_code=404, detail="Task not found")
    
    cursor.execute("DELETE FROM tasks WHERE id = %s", (task_id,))
    db.commit()
    cursor.close()
    
    return Task(**task)



@app.get("/tasks/search/", response_model=List[Task])
def search_tasks(query: str, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute(
        "SELECT * FROM tasks WHERE title LIKE %s OR description LIKE %s",
        (f"%{query}%", f"%{query}%")
    )
    tasks = cursor.fetchall()
    cursor.close()
    return [Task(**task) for task in tasks]

@app.get("/tasks/sort/", response_model=List[Task])
def sort_tasks(sort_by: str = Query('created_at', enum=['created_at', 'title']), db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute(f"SELECT * FROM tasks ORDER BY {sort_by}")
    tasks = cursor.fetchall()
    cursor.close()
    return [Task(**task) for task in tasks]

@app.get("/tasks/priority/", response_model=List[Task])
def get_prioritized_tasks(priority: Optional[int] = None, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    if priority is not None:
        cursor.execute("SELECT * FROM tasks WHERE priority = %s", (priority,))
    else:
        cursor.execute("SELECT * FROM tasks")
    tasks = cursor.fetchall()
    cursor.close()
    return [Task(**task) for task in tasks]

@app.post("/tasks/{task_id}/reminder/", response_model=Task)
def set_reminder(task_id: int, reminder: datetime, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute("UPDATE tasks SET reminder = %s WHERE id = %s", (reminder, task_id))
    db.commit()
    cursor.execute("SELECT * FROM tasks WHERE id = %s", (task_id,))
    updated_task = cursor.fetchone()
    cursor.close()
    if updated_task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return Task(**updated_task)

@app.get("/tasks/overdue/", response_model=List[Task])
def get_overdue_tasks(current_time: datetime, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM tasks WHERE due_date < %s AND completed = FALSE", (current_time,))
    tasks = cursor.fetchall()
    cursor.close()
    return [Task(**task) for task in tasks]

@app.post("/tasks/{task_id}/comment/", response_model=Task)
def add_comment(task_id: int, comment: str, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute("INSERT INTO task_comments (task_id, comment) VALUES (%s, %s)", (task_id, comment))
    db.commit()
    cursor.execute("SELECT * FROM tasks WHERE id = %s", (task_id,))
    task = cursor.fetchone()
    cursor.close()
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return Task(**task)

@app.get("/tasks/comments/{task_id}", response_model=List[str])
def get_task_comments(task_id: int, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT comment FROM task_comments WHERE task_id = %s", (task_id,))
    comments = cursor.fetchall()
    cursor.close()
    return [comment['comment'] for comment in comments]

@app.delete("/tasks/{task_id}/comments/", response_model=List[str])
def delete_task_comments(task_id: int, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute("DELETE FROM task_comments WHERE task_id = %s", (task_id,))
    db.commit()
    cursor.execute("SELECT comment FROM task_comments WHERE task_id = %s", (task_id,))
    remaining_comments = cursor.fetchall()
    cursor.close()
    return [comment['comment'] for comment in remaining_comments]

@app.post("/tasks/{task_id}/assign/", response_model=Task)
def assign_task(task_id: int, user_id: int, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute("UPDATE tasks SET assigned_to = %s WHERE id = %s", (user_id, task_id))
    db.commit()
    cursor.execute("SELECT * FROM tasks WHERE id = %s", (task_id,))
    updated_task = cursor.fetchone()
    cursor.close()
    if updated_task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return Task(**updated_task)

@app.get("/tasks/assigned/{user_id}", response_model=List[Task])
def get_assigned_tasks(user_id: int, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM tasks WHERE assigned_to = %s", (user_id,))
    tasks = cursor.fetchall()
    cursor.close()
    return [Task(**task) for task in tasks]

@app.post("/tasks/duplicate/{task_id}", response_model=Task)
def duplicate_task(task_id: int, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM tasks WHERE id = %s", (task_id,))
    task = cursor.fetchone()
    if task is None:
        cursor.close()
        raise HTTPException(status_code=404, detail="Task not found")

    cursor.execute(
        "INSERT INTO tasks (title, description, completed, priority) VALUES (%s, %s, %s, %s)",
        (task['title'], task['description'], task['completed'], task['priority'])
    )
    db.commit()
    new_task_id = cursor.lastrowid
    cursor.execute("SELECT * FROM tasks WHERE id = %s", (new_task_id,))
    new_task = cursor.fetchone()
    cursor.close()
    return Task(**new_task)

@app.post("/tasks/bulk/", response_model=List[Task])
def bulk_create_tasks(tasks: List[TaskCreate], db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    for task in tasks:
        cursor.execute(
            "INSERT INTO tasks (title, description, completed, priority) VALUES (%s, %s, %s, %s)",
            (task.title, task.description, task.completed, task.priority)
        )
    db.commit()
    cursor.execute("SELECT * FROM tasks ORDER BY id DESC LIMIT %s", (len(tasks),))
    new_tasks = cursor.fetchall()
    cursor.close()
    return [Task(**task) for task in new_tasks]

@app.put("/tasks/bulk/", response_model=List[Task])
def bulk_update_tasks(task_updates: List[TaskUpdate], db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    for task_update in task_updates:
        updates = []
        values = []

        if task_update.title is not None:
            updates.append("title = %s")
            values.append(task_update.title)
        if task_update.description is not None:
            updates.append("description = %s")
            values.append(task_update.description)
        if task_update.completed is not None:
            updates.append("completed = %s")
            values.append(task_update.completed)
        if task_update.priority is not None:
            updates.append("priority = %s")
            values.append(task_update.priority)

        if updates:
            values.append(task_update.id)
            update_str = ", ".join(updates)
            cursor.execute(f"UPDATE tasks SET {update_str} WHERE id = %s", tuple(values))
    db.commit()

    cursor.execute("SELECT * FROM tasks WHERE id IN (%s)" % ','.join(str(t.id) for t in task_updates), tuple(t.id for t in task_updates))
    updated_tasks = cursor.fetchall()
    cursor.close()
    return [Task(**task) for task in updated_tasks]

@app.delete("/tasks/bulk/", response_model=List[Task])
def bulk_delete_tasks(task_ids: List[int], db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute("SELECT * FROM tasks WHERE id IN (%s)" % ','.join(str(id) for id in task_ids), tuple(task_ids))
    tasks = cursor.fetchall()

    if not tasks:
        cursor.close()
        raise HTTPException(status_code=404, detail="Tasks not found")

    cursor.execute("DELETE FROM tasks WHERE id IN (%s)" % ','.join(str(id) for id in task_ids), tuple(task_ids))
    db.commit()
    cursor.close()
    return [Task(**task) for task in tasks]

@app.put("/tasks/{task_id}/priority/", response_model=Task)
def set_task_priority(task_id: int, priority: int, db: MySQLConnection = Depends(get_db)):
    cursor = db.cursor(dictionary=True)
    cursor.execute("UPDATE tasks SET priority = %s WHERE id = %s", (priority, task_id))
    db.commit()
    cursor.execute("SELECT * FROM tasks WHERE id = %s", (task_id,))
    updated_task = cursor.fetchone()
    cursor.close()
    if updated_task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return Task(**updated_task)
