#ifndef PARQUET_FDW_HEAP_HPP
#define PARQUET_FDW_HEAP_HPP

#include <algorithm>
#include <cstddef>
#include <functional> 


template<class T>
class Heap
{
private:
    typedef std::function<bool(const T&, const T&)> cmp_func;

    T      *_data;
    size_t  _size;
    size_t  _capacity;
    cmp_func _cmp;
private:
    void heapify_down(size_t parent)
    {
        while (true)
        {
            size_t smallest = parent;
            size_t left = parent * 2 + 1;
            size_t right = parent * 2 + 2;

            if (left < _size && _cmp(_data[smallest], _data[left]))
                smallest = left;
            if (right < _size && _cmp(_data[smallest], _data[right]))
                smallest = right;

            if (parent == smallest)
                break;

            std::swap(_data[parent], _data[smallest]);
            parent = smallest;
        }
    }
public:
    Heap() :
        _data(nullptr), _size(0), _capacity(0)
    { }

    ~Heap()
    {
        if (_data)
            delete _data;
    }

    T& operator[](int idx)
    {
        return _data[idx];
    }

    void init(size_t capacity, cmp_func cmp)
    {
        if (_data)
            delete _data;
        _data = new T[capacity];
        _capacity = capacity;
        _cmp = cmp;
    }

    size_t size()
    {
        return _size;
    }

    bool empty()
    {
        return _size == 0;
    }

    void clear()
    {
        _size = 0;
    }

    /*
     * Push a new element into heap but do not heapify. Used for initialization.
     */
    void append(const T &new_value)
    {
        /* TODO: assert _size <= _capacity */
        _data[_size++] = new_value;
    }

    /*
     * Rearrange entire heap
     */
    void heapify()
    {
        for (int i = _size / 2 - 1; i >= 0; i--) {
            heapify_down(i);
        }
    }

    T& head()
    {
        /* TODO: assert */
        return _data[0];
    }

    /*
     * Rearrange the heap after head node was changed
     */
    void heapify_head()
    {
        heapify_down(0);
    }

    /*
     * Drop head node and rearrange the heap
     */
    void pop()
    {
        if (_size > 0) {
            std::swap(_data[0], _data[_size - 1]);
            _size--;
            heapify_down(0);
        }
    }
};

#endif
