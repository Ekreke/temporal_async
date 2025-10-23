import { useCallback } from 'react';

function TextNode(props) {
    const onChange = useCallback((evt) => {
        console.log(evt.target.value);
    }, []);

    return (
        <div className="text-node">
            <div>
                <label htmlFor="text">Domain:</label>
                <input id="text" name="text" onChange={onChange} className="nodrag" />
            </div>
        </div>
    );
}

export default TextNode;