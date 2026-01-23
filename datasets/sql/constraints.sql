CREATE TABLE departments (
    id INT PRIMARY KEY,
    name VARCHAR(100) UNIQUE
);

CREATE TABLE students (
    id INT PRIMARY KEY,
    full_name VARCHAR(100),
    birth_year INT,
    grade_level INT
);

CREATE TABLE courses (
    id INT,
    dept_id INT REFERENCES departments(id),
    title VARCHAR(100),
    credits INT,
    PRIMARY KEY (id, dept_id),
    UNIQUE (title, dept_id)
);

CREATE TABLE enrollments (
    student_id INT REFERENCES students(id),
    course_id INT,
    dept_id INT,
    semester VARCHAR(10),
    grade CHAR(2),
    PRIMARY KEY (student_id, course_id, dept_id),
    FOREIGN KEY (course_id, dept_id) REFERENCES courses(id, dept_id)
);

CREATE TABLE instructors (
    id INT PRIMARY KEY,
    full_name VARCHAR(100),
    dept_id INT REFERENCES departments(id),
    email VARCHAR(150) UNIQUE
);

